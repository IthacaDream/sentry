import React from 'react';
import {History} from 'react-router';

import ApiMixin from '../mixins/apiMixin';
import FileSize from '../components/fileSize';
import LoadingError from '../components/loadingError';
import LoadingIndicator from '../components/loadingIndicator';
import IndicatorStore from '../stores/indicatorStore';
import Pagination from '../components/pagination';
import {t} from '../locale';

const ReleaseArtifacts = React.createClass({
  contextTypes: {
    release: React.PropTypes.object
  },

  mixins: [
    ApiMixin,
    History
  ],

  getInitialState() {
    return {
      loading: true,
      error: false,
      fileList: [],
      pageLinks: null
    };
  },

  componentWillMount() {
    this.fetchData();
  },

  componentDidUpdate(prevProps) {
    if (this.props.location.search !== prevProps.location.search) {
      this.fetchData();
    }
  },

  fetchData() {
    let params = this.props.params;
    let endpoint = '/projects/' + params.orgId + '/' + params.projectId + '/releases/' + params.version + '/files/';

    this.setState({
      loading: true,
      error: false
    });

    this.api.request(endpoint, {
      method: 'GET',
      data: this.props.location.query,
      success: (data, _, jqXHR) => {
        this.setState({
          error: false,
          loading: false,
          fileList: data,
          pageLinks: jqXHR.getResponseHeader('Link')
        });
      },
      error: () => {
        this.setState({
          error: true,
          loading: false
        });
      }
    });
  },

  handleRemove(id) {
    let params = this.props.params;
    let endpoint = '/projects/' + params.orgId + '/' + params.projectId + '/releases/' + params.version + '/files/' + id + '/';
    let loadingIndicator = IndicatorStore.add(t('Doing a thing..'));

    this.api.request(endpoint, {
      method: 'DELETE',
      success: (data, _, jqXHR) => {
        let fileList = this.state.fileList.filter((file) => {
          return file.id !== id;
        });

        this.setState({
          fileList: fileList
        });
        IndicatorStore.add(t('Done a thing...'), 'success');
      },
      complete: () => {
        IndicatorStore.remove(loadingIndicator);
      }
    });
  },

  render() {
    if (this.state.loading)
      return <LoadingIndicator />;
    else if (this.state.error)
      return <LoadingError onRetry={this.fetchData} />;
    else if (this.state.fileList.length === 0)
      return (
        <div className="box empty-stream">
          <span className="icon icon-exclamation"></span>
          <p>{t('There are no artifacts uploaded for this release.')}</p>
        </div>
      );

    // TODO(dcramer): files should allow you to download and delete them
    return (
      <div>
        <div className="release-group-header">
          <div className="row">
            <div className="col-sm-9 col-xs-5">{'Name'}</div>
            <div className="col-sm-1 col-xs-3 align-right">{'Size'}</div>
            <div className="col-sm-2 col-xs-1 align-right"></div>
          </div>
        </div>
        <div className="release-list">
        {this.state.fileList.map((file) => {
          return (
            <div className="release release-artifact row" key={file.id}>
              <div className="col-sm-9 col-xs-5" style={{wordWrap: 'break-word'}}><strong>{file.name || '(empty)'}</strong></div>
              <div className="col-sm-1 col-xs-3 align-right"><FileSize bytes={file.size} /></div>
              <div className="col-sm-2 col-xs-1 align-right">
                <a className="btn btn-sm btn-default" onClick={this.handleRemove.bind(this, file.id)}>
                  <span className="icon icon-trash" /> &nbsp;{t('Remove')}
                </a>
              </div>
            </div>
          );
        })}
        </div>
        <Pagination pageLinks={this.state.pageLinks}/>
      </div>
    );
  }
});

export default ReleaseArtifacts;
