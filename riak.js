/**
 * @author Craig Thayer <cthayer@sazze.com>
 * @copyright 2014 Sazze, Inc.
 */

var _ = require('lodash');
var client = require('request');

var AURA_DEFINED = !_.isUndefined(global.aura);

var log = (!AURA_DEFINED || _.isUndefined(global.aura.log) ? {error: console.error, warn: console.warn, info: console.info, debug: _.noop, verbose: _.noop} : global.aura.log);

function Riak(bucket) {
  this.bucket = bucket;
  this.host = process.env.SZ_RIAK_HOST || (AURA_DEFINED && !_.isUndefined(global.aura.config.riak) ? global.aura.config.riak.host : '127.0.0.1');
  this.port = process.env.SZ_RIAK_PORT || (AURA_DEFINED && !_.isUndefined(global.aura.config.riak) ? global.aura.config.riak.port : 8098);
}

//
// static methods
//

Riak.parseHeaders = function (headers) {
  var properties = {
    vclock: '',
    links: [],
    meta: {},
    index: {},
    contentType: ''
  };

  if (_.isEmpty(headers)) {
    return properties;
  }

  _.forEach(headers, function (val, name) {
    switch (name.toLowerCase()) {
      case 'content-type':
        properties.contentType = val;
        break;

      case 'x-riak-vclock':
        properties.vclock = val;
        break;

      case 'link':
        val = val.split(',');

        _.forEach(val, function (l) {
          var parts = l.trim().split(';');
          var link = {link: parts[0].trim().replace('<', '').replace('>', ''), tag: ''};

          if (!_.isUndefined(parts[1])) {
            link.tag = parts[1].trim().replace('riaktag="', '').replace('"', '');
          }

          properties.links.push(link);
        });

        break;

      default:
        if (_.contains(name, 'x-riak-meta-')) {
          properties.meta[name.replace('x-riak-meta-', '')] = val;
          break;
        }

        if (_.contains(name, 'x-riak-index-')) {
          if (_.contains(name, 'x-riak-index-')) {
            properties.index[name.replace('x-riak-index-', '')] = val;
            break;
          }
        }

        break;
    }
  });

  log.verbose(properties);

  return properties;
};

Riak.prepareHeaders = function (obj) {
  var headers = {};

  if (_.isEmpty(obj)) {
    return headers;
  }

  var links = [];

  _.forEach(obj, function (val, name) {
    switch (name.toLowerCase()) {
      case 'vclock':
        headers['x-riak-vclock'] = val;

        delete obj.vclock;

        break;

      case 'links':
        _.forEach(obj.links, function (link) {
          if (_.isUndefined(link.link)) {
            return;
          }

          if (_.isUndefined(link.tag) || _.isEmpty(link.tag)) {
            links.push('<' + link.link + '>');
          } else {
            links.push('<' + link.link + '>; riaktag="' + link.tag + '"');
          }
        });

        delete obj.links;

        break;

      case 'meta':
        _.forEach(obj.meta, function (val, name) {
          headers['x-riak-meta-' + name] = val;
        });

        delete obj.meta;

        break;

      case 'index':
        _.forEach(obj.index, function (val, name) {
          headers['x-riak-index-' + name] = val;
        });

        delete obj.index;

        break;

      default:
        break;
    }
  });

  if (links.length > 0) {
    headers['link'] = links.join(', ');
  }

  log.verbose(headers);

  return headers;
};

module.exports = Riak;

//
// methods
//

Riak.prototype.getUrl = function (key) {
  return 'http://' + this.host + ':' + this.port + '/buckets/' + this.bucket + '/keys/' + key;
};

Riak.prototype.get = function (key, cb) {
  if (!_.isFunction(cb)) {
    cb = _.noop;
  }

  if (!_.isString(key) || !key) {
    cb(new Error('Invalid key: ' + key));
    return;
  }

  var url = this.getUrl(key);

  log.verbose('riak url: GET ' + url);

  client(url, function (err, resp, body) {
    if (err) {
      cb(err);
      return;
    }

    log.debug('riak response code: ' + resp.statusCode);
    log.verbose('riak headers: ' + JSON.stringify(resp.headers));
    log.verbose('riak response: ' + body);

    if (resp.statusCode == 404) {
      // handle not found response
      cb(null, {});
      return;
    }

    if (resp.statusCode != 200) {
      cb(new Error('riak returned status code: ' + resp.statusCode));
      return;
    }

    if (_.isString(body) && resp.headers['content-type'].toLowerCase() == 'application/json') {
      body = JSON.parse(body);
    }

    _.merge(body, Riak.parseHeaders(resp.headers));

    cb(null, body, resp.headers);
  });
};

Riak.prototype.put = function (key, body, headers, cb) {
  if (_.isUndefined(headers)) {
    headers = {};
  }

  if (_.isFunction(headers)) {
    cb = headers;
    headers = {};
  }

  if (!_.isFunction(cb)) {
    cb = _.noop;
  }

  if (!_.isString(key) || !key) {
    cb(new Error('Invalid key: ' + key));
    return;
  }

  // if the body is not a string, we treat it as an object and store with content-type: application/json
  if (!_.isString(body)) {
    headers['content-type'] = 'application/json';

    if (_.isObject(body)) {
      _.merge(headers, Riak.prepareHeaders(body));
    }

    body = JSON.stringify(body);
  }

  if (_.isUndefined(headers['content-type'])) {
    headers['content-type'] = 'text/plain';
  }

  var url = this.getUrl(key);

  log.verbose('riak url: PUT ' + url);

  client.put({url: url, body: body, headers: headers, qs: {returnbody: true}}, function (err, resp, body) {
    if (err) {
      cb(err);
      return;
    }

    log.debug('riak response code: ' + resp.statusCode);
    log.verbose('riak headers: ' + JSON.stringify(resp.headers));
    log.verbose('riak response: ' + body);

    if (resp.statusCode != 200) {
      cb(new Error('riak returned status code: ' + resp.statusCode));
      return;
    }

    if (_.isString(body) && resp.headers['content-type'].toLowerCase() == 'application/json') {
      body = JSON.parse(body);
    }

    _.merge(body, Riak.parseHeaders(resp.headers));

    cb(null, body, resp.headers);
  });
};

Riak.prototype.del = function (key, cb) {
  if (!_.isFunction(cb)) {
    cb = _.noop;
  }

  if (!_.isString(key) || !key) {
    cb(new Error('Invalid key: ' + key));
    return;
  }

  var url = this.getUrl(key);

  log.verbose('riak url: DELETE ' + url);

  client.del(url, function (err, resp) {
    if (err) {
      cb(err);
      return;
    }

    log.debug('riak response code: ' + resp.statusCode);
    log.verbose('riak headers: ' + JSON.stringify(resp.headers));

    if (resp.statusCode == 404) {
      // handle not found response
      cb(null);
      return;
    }

    if (resp.statusCode != 204) {
      cb(new Error('riak returned status code: ' + resp.statusCode));
      return;
    }

    cb(null);
  });
};