var subject = require("../lib/entity")
var assert      = require('assert');
var slugid      = require('slugid');
var _           = require('lodash');
var Promise     = require('promise');
var azureTable  = require('azure-table-node');
var helper      = require('./helper');

suite("Entity (Shared-Access-Signatures)", function() {

  var credentials = helper.cfg.azure;

  credentials = _.defaults({}, credentials, {
    accountUrl: [
      "https://",
      credentials.accountName,
      ".table.core.windows.net/"
    ].join('')
  });
  var client = azureTable.createClient(credentials);
  var sas = client.generateSAS(
    helper.cfg.tableName,
    'raud',
    new Date(Date.now() + 15 * 60 * 1000),
    {
      start:  new Date(Date.now() - 15 * 60 * 1000)
    }
  );

  var Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.StringKey('id'),
    rowKey:           subject.keys.StringKey('name'),
    properties: {
      id:             subject.types.String,
      name:           subject.types.String,
      count:          subject.types.Number
    }
  }).setup({
    credentials: {
      accountName:    helper.cfg.azure.accountName,
      sas:            sas
    },
    table:            helper.cfg.tableName
  });

  test("Item.create, item.modify, item.reload", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        assert(itemA !== itemB);
        return itemB.modify(function() {
          this.count += 1;
        });
      }).then(function() {
        assert(itemA.count === 1);
        return itemA.reload();
      }).then(function(updated) {
        assert(updated);
        assert(itemA.count === 2);
      }).then(function() {
        return itemA.reload();
      }).then(function(updated) {
        assert(!updated);
        assert(itemA.count === 2);
      });
    });
  });
});
