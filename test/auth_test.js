var subject         = require('../lib/entity');
var assert          = require('assert');
var slugid          = require('slugid');
var _               = require('lodash');
var Promise         = require('promise');
var debug           = require('debug')('test:entity:auth');
var express         = require('express');
var azureTable      = require('azure-table-node');
var helper          = require('./helper');
var API             = require('taskcluster-lib-api');
var testing         = require('taskcluster-lib-testing');
var _validate       = require('taskcluster-lib-validate');
var _app            = require('taskcluster-lib-app');

suite('Entity (SAS from auth.taskcluster.net)', function() {
  // Create test api
  var api = new API({
    title:        'Test TC-Auth',
    description:  'Another test api',
  });

  // Declare a method we can test parameterized scopes with
  var returnExpiredSAS = false;
  var callCount = 0;
  api.declare({
    method:     'get',
    route:      '/azure/:account/table/:table/:level',
    name:       'azureTableSAS',
    deferAuth:  true,
    scopes:     [['auth:azure-table:<level>:<account>/<table>']],
    title:        'Test SAS End-Point',
    description:  'Get SAS for testing',
  }, function(req, res) {
    callCount += 1;
    var account = req.params.account;
    var table   = req.params.table;
    var level   = req.params.level;
    if (!req.satisfies({account: account, table: table, level: level})) {
      return;
    }
    var credentials = helper.cfg.azure;
    assert(account === credentials.accountName, 'Must used test account!');
    credentials = _.defaults({}, credentials, {
      accountUrl: [
        'https://',
        credentials.accountName,
        '.table.core.windows.net/',
      ].join(''),
    });
    var client = azureTable.createClient(credentials);
    var expiry = new Date(Date.now() + 25 * 60 * 1000);
    // Return and old expiry, this causes a refresh on the next call
    if (returnExpiredSAS) {
      expiry = new Date(Date.now() + 15 * 60 * 1000 + 100);
    }
    var sas = client.generateSAS(
      table,
      'raud',
      expiry,
      {
        start:  new Date(Date.now() - 15 * 60 * 1000),
      }
    );
    res.status(200).json({
      expiry:   expiry.toJSON(),
      sas:      sas,
    });
  });

  // Create servers
  var server = null;
  setup(async function() {
    testing.fakeauth.start({
      'authed-client': ['*'],
      'unauthed-client': ['*'],
    });
    var validator = await _validate({
      folder: 'test/schemas',
      prefix: 'test/v1',
    });

    // Create a simple app
    var app = _app({
      port:       23244,
      env:        'development',
      forceSSL:   false,
      trustProxy: false,
    });

    app.use(api.router({
      validator:      validator,
    }));

    server = await app.createServer();
  });

  teardown(async function() {
    await server.terminate();
    testing.fakeauth.stop();
  });

  var ItemV1;
  test('ItemV1 = Entity.configure', function() {
    ItemV1 = subject.configure({
      version:          1,
      partitionKey:     subject.keys.StringKey('id'),
      rowKey:           subject.keys.StringKey('name'),
      properties: {
        id:             subject.types.String,
        name:           subject.types.String,
        count:          subject.types.Number,
      },
    });
  });

  var Item;
  test('Item = ItemV1.setup', function() {
    Item = ItemV1.setup({
      account:      helper.cfg.azure.accountName,
      table:        helper.cfg.tableName,
      credentials:  {
        clientId:         'authed-client',
        accessToken:      'test-token',
      },
      authBaseUrl:  'http://localhost:23244',
      minSASAuthExpiry: 15 * 60 * 1000,
    });
  });

  test('Item.create && Item.load', function() {
    var id = slugid.v4();
    callCount = 0;
    returnExpiredSAS = false; // We should be able to reuse the SAS
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    }).then(function() {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(item) {
        assert(item.count === 1);
      });
    }).then(function() {
      assert(callCount === 1, 'We should only have called once!');
    });
  });

  test('Expiry < now => refreshed SAS', function() {
    callCount = 0;
    returnExpiredSAS = true;  // This means we call for each operation
    var id = slugid.v4();
    var Item2 = ItemV1.setup({
      account:      helper.cfg.azure.accountName,
      table:        helper.cfg.tableName,
      credentials:  {
        clientId:         'authed-client',
        accessToken:      'test-token',
      },
      authBaseUrl:  'http://localhost:23244',
    });
    return Item2.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    }).then(function() {
      assert(callCount === 1, 'We should only have called once!');
      return testing.sleep(200);
    }).then(function() {
      return Item2.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(item) {
        assert(item.count === 1);
      });
    }).then(function() {
      assert(callCount === 2, 'We should have called twice!');
    });
  });

  test('Load in parallel, only gets SAS once', function() {
    callCount = 0;
    returnExpiredSAS = false;  // This means we call for each operation
    var Item3 = ItemV1.setup({
      account:      helper.cfg.azure.accountName,
      table:        helper.cfg.tableName,
      credentials:  {
        clientId:         'authed-client',
        accessToken:      'test-token',
      },
      authBaseUrl:  'http://localhost:23244',
    });
    return Promise.all([
      Item3.create({
        id:     slugid.v4(),
        name:   'my-test-item1',
        count:  1,
      }),
      Item3.create({
        id:     slugid.v4(),
        name:   'my-test-item2',
        count:  1,
      }),
    ]).then(function() {
      assert(callCount === 1, 'We should only have called once!');
    });
  });
});
