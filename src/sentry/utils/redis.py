from __future__ import absolute_import

import posixpath
from pkg_resources import resource_string
from threading import Lock

import rb
from redis.connection import ConnectionPool
from redis.client import Script

from sentry.exceptions import InvalidConfiguration
from sentry.utils.versioning import (
    Version,
    check_versions,
)


_pool_cache = {}
_pool_lock = Lock()


def _shared_pool(**opts):
    if 'host' in opts:
        key = '%s:%s/%s' % (
            opts['host'],
            opts['port'],
            opts['db'],
        )
    else:
        key = '%s/%s' % (
            opts['path'],
            opts['db']
        )
    pool = _pool_cache.get(key)
    if pool is not None:
        return pool
    with _pool_lock:
        pool = _pool_cache.get(key)
        if pool is not None:
            return pool
        pool = ConnectionPool(**opts)
        _pool_cache[key] = pool
        return pool


def make_rb_cluster(hosts=None, options=None):
    """Returns a rb cluster that internally shares the pools more
    intelligently.
    """
    options = dict(options or ())
    if hosts is not None:
        options['hosts'] = hosts
    kwargs = {}
    for key in 'hosts', 'host_defaults', 'pool_options', 'router_options':
        val = options.get(key)
        if val is not None:
            kwargs[key] = val
    kwargs['pool_cls'] = _shared_pool
    return rb.Cluster(**kwargs)


class ClusterManager(object):
    def __init__(self):
        self.__clusters = {}

    def get(self, key):
        # XXX: Circular import, fix me.
        from sentry import options

        cluster = self.__clusters.get(key)

        if cluster is None:
            # Try and get the configuration for the named cluster first.
            configurations = options.get('redis.clusters')
            configuration = configurations.get(key)

            # If there is no configuration for that cluster, use the default.
            # TODO: Probably just initialize the default cluster once?
            # TODO: It might be helpful to log this case for debugging?
            if configuration is None:
                configuration = configurations['default']

            cluster = self.__clusters[key] = rb.Cluster(
                pool_cls=_shared_pool,
                **configuration
            )

        return cluster


clusters = ClusterManager()


def check_cluster_versions(cluster, required, recommended=Version((3, 0, 4)), label=None):
    try:
        with cluster.all() as client:
            results = client.info()
    except Exception as e:
        # Any connection issues should be caught here.
        raise InvalidConfiguration(unicode(e))

    versions = {}
    for id, info in results.value.items():
        host = cluster.hosts[id]
        # NOTE: This assumes there is no routing magic going on here, and
        # all requests to this host are being served by the same database.
        key = '{host}:{port}'.format(host=host.host, port=host.port)
        versions[key] = Version(map(int, info['redis_version'].split('.', 3)))

    check_versions(
        'Redis' if label is None else 'Redis (%s)' % (label,),
        versions,
        required,
        recommended,
    )


def load_script(path):
    script = Script(None, resource_string('sentry', posixpath.join('scripts', path)))

    # This changes the argument order of the ``Script.__call__`` method to
    # encourage using the script with a specific Redis client, rather
    # than implicitly using the first client that the script was registered
    # with. (This can prevent lots of bizzare behavior when dealing with
    # clusters of Redis servers.)
    def call_script(client, keys, args):
        """
        Executes {!r} as a Lua script on a Redis server.

        Takes the client to execute the script on as the first argument,
        followed by the values that will be provided as ``KEYS`` and ``ARGV``
        to the script as two sequence arguments.
        """.format(path)
        return script(keys, args, client)

    return call_script
