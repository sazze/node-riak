var expect = require('chai').expect;
var _ = require('lodash');
var os = require('os');

var Riak = require('../');
var bucket = 'sz-riak-test.' + os.hostname() + os.uptime();

var riakHost = process.env.SZ_RIAK_HOST || '127.0.0.1';

describe('riak client', function () {
  it('should export', function () {
    expect(Riak).to.be.a('function');
    expect(Riak).to.have.property('parseHeaders');
    expect(Riak.parseHeaders).to.be.a('function');
    expect(Riak).to.have.property('prepareHeaders');
    expect(Riak.prepareHeaders).to.be.a('function');
    expect(Riak).to.have.property('ASYNC_LIMIT');
    expect(Riak.ASYNC_LIMIT).to.be.a('number');
    expect(Riak.ASYNC_LIMIT).to.be.equal(20);
  });

  it('should initialize', function () {
    var riak = new Riak(bucket);

    expect(riak).to.be.an('object');
    expect(riak).to.have.property('bucket');
    expect(riak.bucket).to.be.a('string');
    expect(riak.bucket).to.equal(bucket);
    expect(riak).to.have.property('host');
    expect(riak.host).to.be.a('string');
    expect(riak.host).to.equal(riakHost);
    expect(riak).to.have.property('port');
    expect(riak.port).to.be.a('number');
    expect(riak.port).to.equal(8098);
    expect(riak).to.have.property('getUrl');
    expect(riak.getUrl).to.be.a('function');
    expect(riak.getUrl(1)).to.equal('http://' + riakHost + ':8098/buckets/' + bucket + '/keys/1');
    expect(riak).to.have.property('getSecondaryIndexUrl');
    expect(riak.getSecondaryIndexUrl).to.be.a('function');
    expect(riak.getSecondaryIndexUrl('index', 1)).to.equal('http://' + riakHost + ':8098/buckets/' + bucket + '/index/index/1');
    expect(riak).to.have.property('get');
    expect(riak.get).to.be.a('function');
    expect(riak).to.have.property('put');
    expect(riak.put).to.be.a('function');
    expect(riak).to.have.property('del');
    expect(riak.del).to.be.a('function');
    expect(riak).to.have.property('mget');
    expect(riak.mget).to.be.a('function');
    expect(riak).to.have.property('mput');
    expect(riak.mput).to.be.a('function');
    expect(riak).to.have.property('mdel');
    expect(riak.mdel).to.be.a('function');
    expect(riak).to.have.property('secondaryIndexSearch');
    expect(riak.secondaryIndexSearch).to.be.a('function');
  });

  it('should parse headers', function () {
    var tests = [
      {
        test: {},
        result: {
          vclock: '',
          links: [],
          meta: {},
          index: {},
          contentType: ''
        }
      },
      {
        test: {
          'content-type': 'application/json',
          'x-riak-vclock': 'some.vclock_value:1234567 Blah blah'
        },
        result: {
          vclock: 'some.vclock_value:1234567 Blah blah',
          links: [],
          meta: {},
          index: {},
          contentType: 'application/json'
        }
      },
      {
        test: {
          'content-type': 'application/json',
          'x-riak-vclock': 'some.vclock_value:1234567 Blah blah',
          link: '</riak/list/1>; riaktag="previous", </riak/list/3>; riaktag="next", </riak/list/60>',
          'x-riak-meta-some_name': 'some value',
          'x-riak-meta-another_name': 'another value',
          'x-riak-index-some_name': 'some value',
          'x-riak-index-another_name': 'another value'
        },
        result: {
          vclock: 'some.vclock_value:1234567 Blah blah',
          links: [
            {
              link: '/riak/list/1',
              tag: 'previous'
            },
            {
              link: '/riak/list/3',
              tag: 'next'
            },
            {
              link: '/riak/list/60',
              tag: ''
            }
          ],
          meta: {
            some_name: 'some value',
            another_name: 'another value'
          },
          index: {
            some_name: 'some value',
            another_name: 'another value'
          },
          contentType: 'application/json'
        }
      }
    ];

    _.forEach(tests, function (test) {
      expect(Riak.parseHeaders(test.test)).to.eql(test.result);
    });
  });

  it('should prepare headers', function () {
    var tests = [
      {
        test: {},
        result: {}
      },
      {
        test: {
          vclock: 'some.vclock_value:1234567 Blah blah',
          links: [],
          meta: {},
          index: {}
        },
        result: {
          'x-riak-vclock': 'some.vclock_value:1234567 Blah blah'
        }
      },
      {
        test: {
          vclock: 'some.vclock_value:1234567 Blah blah',
          links: [
            {
              link: '/riak/list/1',
              tag: 'previous'
            },
            {
              link: '/riak/list/3',
              tag: 'next'
            },
            {
              link: '/riak/list/60',
              tag: ''
            }
          ],
          meta: {
            some_name: 'some value',
            another_name: 'another value'
          },
          index: {
            some_name: 'some value',
            another_name: 'another value'
          }
        },
        result: {
          'x-riak-vclock': 'some.vclock_value:1234567 Blah blah',
          link: '</riak/list/1>; riaktag="previous", </riak/list/3>; riaktag="next", </riak/list/60>',
          'x-riak-meta-some_name': 'some value',
          'x-riak-meta-another_name': 'another value',
          'x-riak-index-some_name': 'some value',
          'x-riak-index-another_name': 'another value'
        }
      }
    ];

    _.forEach(tests, function (test) {
      expect(Riak.prepareHeaders(test.test)).to.eql(test.result);
    });
  });

  it('should manipulate keys', function (done) {
    var riak = new Riak(bucket);
    var obj = {
      test: 'testing',
      some: 'value'
    };

    riak.put('test1', obj, function (err, body, headers) {
      var parsedHeaders = Riak.parseHeaders(headers);

      expect(err).to.equal(null);
      expect(headers['content-type']).to.equal('application/json');
      expect(body).to.eql(_.merge(obj, parsedHeaders));

      riak.get('test1', function (err, body, headers) {
        var parsedHeaders = Riak.parseHeaders(headers);

        expect(err).to.equal(null);
        expect(headers['content-type']).to.equal('application/json');
        expect(body).to.eql(_.merge(obj, parsedHeaders));

        riak.del('test1', function (err) {
          done(err);
        });
      });
    });
  });

  it('should manipulate multiple keys', function (done) {
    var riak = new Riak(bucket);
    var obj = {
      test: 'testing',
      some: 'value'
    };
    var keys = [
      'test1',
      'test2',
      'test3'
    ];
    var puts = [
      {key: 'test1', body: obj},
      {key: 'test2', body: obj},
      {key: 'test3', body: obj}
    ];

    riak.mput(puts, function (err, resp) {
      expect(err).to.equal(undefined);

      _.forEach(resp, function (r) {
        var parsedHeaders = Riak.parseHeaders(r.headers);

        expect(r.headers['content-type']).to.equal('application/json');
        expect(r.resp).to.eql(_.merge(obj, parsedHeaders));
      });

      riak.mget(keys, function (err, resp) {
        expect(err).to.equal(undefined);

        _.forEach(resp, function (r) {
          var parsedHeaders = Riak.parseHeaders(r.headers);

          expect(r.headers['content-type']).to.equal('application/json');
          expect(r.resp).to.eql(_.merge(obj, parsedHeaders));
        });

        riak.mdel(keys, function (err) {
          done(err);
        });
      });
    });
  });

  it('should query secondary indexes', function(done) {
    var riak = new Riak(bucket);

    riak.secondaryIndexSearch('test_bin', 'foo', function (err, res) {
      //expect(err.message).to.contain('status code: 500');
      //expect(res).to.be.a('string');
      //expect(res).to.contain('indexes_not_supported');
      
      done();
    });
  });
});
