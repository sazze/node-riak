/**
 * @author Craig Thayer <cthayer@sazze.com>
 * @copyright 2014 Sazze, Inc.
 */

var _ = require('lodash');
var client = require('request');
var async = require('async');
var querystring = require('querystring');

var log = {};
log.debug = require('debug')('@sazze/riak:debug');
log.verbose = require('debug')('@sazze/riak:verbose');

function Riak(options) {
  if (_.isString(options)) {
    options = {
      bucket: options
    };
  }

  if (!_.isPlainObject(options)) {
    options = {};
  }

  this.bucket = options.bucket || '';
  this.host = options.host || process.env.SZ_RIAK_HOST || '127.0.0.1';
  this.port = options.port || process.env.SZ_RIAK_PORT || 8098;
}

Riak.ASYNC_LIMIT = process.env.SZ_RIAK_ASYNC_LIMIT || 20;

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
            // skip internal riak headers that are not allowed client requests
            if (_.contains(parts[1], 'rel=')) {
              return;
            }

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

Riak.prototype.getSecondaryIndexUrl = function (index, search, options) {
  var url = 'http://' + this.host + ':' + this.port + '/buckets/' + this.bucket + '/index/' + index + '/' + (search.join ? search.join('/') : search);

  if (options && _.isPlainObject(options)) {
    url += '?' + querystring.stringify(options);
  }

  return url;
};

Riak.prototype.mget = function (keys, cb) {
  if (!_.isArray(keys)) {
    cb(new Error('keys must be an array'));
    return;
  }

  async.mapLimit(keys, Riak.ASYNC_LIMIT, function (key, callback) {
    this.get(key, function (err, res, resp) {
      callback(err, {resp: res, rawResp: resp});
    });
  }.bind(this), function (err, results) {
    cb(err, results);
  });
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
      cb(null, {}, resp);
      return;
    }

    if (resp.statusCode != 200) {
      cb(new Error('riak returned status code: ' + resp.statusCode), null, resp);
      return;
    }

    if (_.isString(body) && resp.headers['content-type'].toLowerCase() == 'application/json') {
      body = JSON.parse(body);
    }

    _.merge(body, Riak.parseHeaders(resp.headers));

    cb(null, body, resp);
  });
};

Riak.prototype.mput = function (puts, cb) {
  if (!_.isArray(puts)) {
    cb(new Error('puts must be an array of put objects'));
    return;
  }

  async.mapLimit(puts, Riak.ASYNC_LIMIT, function (put, callback) {
    if (!_.isPlainObject(put)) {
      callback(new Error('Invalid request object: ' + put));
      return;
    }

    this.put(put.key, put.body, put.headers, function (err, res, resp) {
      callback(err, {resp: res, rawResp: resp});
    });
  }.bind(this), function (err, results) {
    cb(err, results);
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
      cb(new Error('riak returned status code: ' + resp.statusCode), null, resp);
      return;
    }

    if (_.isString(body) && resp.headers['content-type'].toLowerCase() == 'application/json') {
      body = JSON.parse(body);
    }

    _.merge(body, Riak.parseHeaders(resp.headers));

    cb(null, body, resp);
  });
};

Riak.prototype.mdel = function (keys, cb) {
  if (!_.isArray(keys)) {
    cb(new Error('keys must be an array'));
    return;
  }

  async.mapLimit(keys, Riak.ASYNC_LIMIT, this.del.bind(this), function (err) {
    cb(err);
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

Riak.prototype.secondaryIndexSearch = function (index, search, options, cb) {
  if (_.isFunction(options)) {
    cb = options;
    options = undefined;
  }

  if (!_.isFunction(cb)) {
    cb = _.noop;
  }

  if (!_.isString(index) || !index) {
    cb(new Error('Invalid index: ' + index));
    return;
  }

  var url = this.getSecondaryIndexUrl(index, search, options);

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
      cb(null, {}, resp);
      return;
    }

    if (resp.statusCode != 200) {
      cb(new Error('riak returned status code: ' + resp.statusCode), body, resp);
      return;
    }

    if (_.isString(body) && resp.headers['content-type'].toLowerCase() == 'application/json') {
      body = JSON.parse(body);
    }

    cb(null, body, resp);
  });
};