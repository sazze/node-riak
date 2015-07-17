@sazze/riak [![Build Status](https://travis-ci.org/sazze/node-riak.svg?branch=master%2Fmaster)](https://travis-ci.org/sazze/node-riak)
====================

A [Riak][0] HTTP Rest API client.

This client is known to work with version 1.4.9 of [Riak][0]

Usage
====================

### Node

``` js
    var Riak = require('@sazze/riak');

    var options = {
      host: '127.0.0.1',
      port: 8098,
      bucket: 'test'
    };
    
    var riak = new Riak(options);
    
    riak.get('key', function (err, obj, headers) {
        // do something
    });
```

Install
====================

``` bash
npm install @sazze/riak
```

Methods
====================

* `get(key, cb)`: get an object from riak
    * `key`: [string] the key of the object to get
    * `cb`: [function] callback function.  Accepts 3 arguments: `err`, `obj`, and `headers`.

* `put(key, body, headers, cb)`: create/update an object in riak
    * `key`: [string] the key of the object to get
    * `body`: [string|object] the data or object to create/update in riak
    * `headers`: [object] \(optional) custom headers to store with the object
    * `cb`: [function] callback function.  Accepts 3 arguments: `err`, `obj`, and `headers`.

* `del(key, cb)`: delete an object from riak
    * `key`: [string] the key of the object to get
    * `cb`: [function] callback function.  Accepts 1 arguments: `err`.

* `secondaryIndexSearch(index, search, options, cb)`: perform a secondary index search
    * `index`: [string] the name of the index to search
    * `search`: [string] the search query
    * `options`: [array] \(optional) search options
    * `cb`: [function] callback function.  Accepts 2 arguments: `err` and `resp`.

* `mget(keys, cb)`: get multiple objects from riak
    * `keys`: [array] key names to get
    * `cb`: [function] callback function.  Accepts 2 arguments: `err` and `resp`.

* `mput(puts, cb)`: create/update multiple objects in riak
    * `puts`: [array] put objects whose members match the `put` function arguments
    * `cb`: [function] callback function. Accepts 2 arguments: `err` and `resp`.

* `mdel(keys, cb)`: delete multiple objects in riak
    * `keys`: [array] key names to delete
    * `cb`: [function] callback function.  Accepts 1 argument: `err`.


Environment Variables
====================

This module allows certain configuration to be obtained from environment variables.  Configuration passed with the `options` object overrides environment variables.

* `SZ_RIAK_HOST`: the riak host to connect to
* `SZ_RIAK_PORT`: the riak port to connect to
* `SZ_RIAK_ASYNC_LIMIT`: the maximum number of requests to perform in parallel (mostly affect the `m*` functions like `mget`)

Run Tests
====================

```
  npm test
```

There must be a local instance of riak running for tests to succeed.  Or point to a remote riak instance by setting the `SZ_RIAK_HOST` environment variable.

Troubleshooting
====================

* **I get an error when installing node packages *"ERR! Error: No compatible version found: assertion-error@'^1.0.1'"***

  If you are running a version of NodeJS less than or equal to 0.8, upgrading NPM to a version greater than or equal to 1.4.6 should solve this issue.

  ```
  npm install -g npm@~1.4.6
  ```

  Another way around is to simply avoid installing the development dependencies:

  ```
  npm install --production
  ```

====================

#### Author: [Craig Thayer](https://github.com/sazze)

#### License: MIT

See LICENSE for the full license text.

[0]: http://basho.com/products/riak-kv/