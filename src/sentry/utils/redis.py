from __future__ import absolute_import

import functools
import posixpath
import warnings
from pkg_resources import resource_string
from threading import Lock

import rb
from redis.connection import ConnectionPool
from redis.client import Script

from sentry.exceptions import InvalidConfiguration
from sentry import options
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


_make_rb_cluster = functools.partial(rb.Cluster, pool_cls=_shared_pool)


def make_rb_cluster(*args, **kwargs):
    warnings.warn(
        'Direct Redis cluster construction is deprecated, please use named clusters.',
        DeprecationWarning,
    )
    return _make_rb_cluster(*args, **kwargs)


class ClusterManager(object):
    def __init__(self):
        self.__clusters = {}

    def get(self, key):
        # TODO: This might need a lock?
        cluster = self.__clusters.get(key)

        if cluster is None:
            # Try and get the configuration for the named cluster first.
            configurations = options.get('redis.clusters')
            configuration = configurations.get(key)

            # If there is no configuration for that cluster, use the default.
            # TODO: Probably just initialize the default cluster once?
            # TODO: It might be helpful to log this case for debugging? Also,
            # should we even allow falling back to an implicit default cluster?
            # This seems like a good way to hide broken configurations.
            if configuration is None:
                configuration = configurations['default']

            cluster = self.__clusters[key] = _make_rb_cluster(**configuration)

        return cluster


clusters = ClusterManager()


def get_cluster_from_options(backend, options, cluster_manager=clusters, cluster_option_name='cluster', default_cluster_name='default'):
    cluster_constructor_option_names = frozenset(('hosts', 'host_defaults', 'pool_options', 'router_options'))

    options = options.copy()
    cluster_options = {key: options.pop(key) for key in set(options.keys()).intersection(cluster_constructor_option_names)}
    if cluster_options:
        if cluster_option_name in options:
            raise InvalidConfiguration(
                'Cannot provide both named cluster ({!r}) and cluster configuration ({!r}) options.'.format(
                    cluster_option_name,
                    cluster_constructor_option_names,
                )
            )
        else:
            warnings.warn(
                'Providing Redis cluster configuration options ({!r}) to {!r} is '
                'deprecated, please update your configuration to use named Redis '
                'clusters ({!r}).'.format(
                    cluster_constructor_option_names,
                    backend,
                    cluster_option_name,
                ),
                DeprecationWarning,
                stacklevel=2
            )
        cluster = rb.Cluster(pool_cls=_shared_pool, **cluster_options)
    else:
        cluster = cluster_manager.get(options.pop(cluster_option_name, default_cluster_name))

    return cluster, options


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
