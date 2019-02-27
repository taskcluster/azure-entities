var subject = require('../src/entity');
var assert      = require('assert');
var slugid      = require('slugid');
var _           = require('lodash');
var azure       = require('fast-azure-storage');
var helper      = require('./helper');

suite('Entity (Shared-Access-Signatures)', function() {
  const table = new azure.Table({
    accountId: helper.cfg.azure.accountId,
    accessKey: helper.cfg.azure.accessKey,
  });
  const sas = table.sas(helper.cfg.tableName, {
    start: new Date(Date.now() - 15 * 60 * 1000),
    expiry: new Date(Date.now() + 15 * 60 * 1000),
    permissions: {
      read: true,
      add: true,
      update: true,
      delete: true,
    },
  });

  var Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.StringKey('id'),
    rowKey:           subject.keys.StringKey('name'),
    properties: {
      id:             subject.types.String,
      name:           subject.types.String,
      count:          subject.types.Number,
    },
  }).setup({
    credentials: {
      accountId:      helper.cfg.azure.accountId,
      sas:            sas,
    },
    tableName:        helper.cfg.tableName,
  });

  test('ensureTable doesn\'t fail', async function() {
    return Item.ensureTable();
  });

  test('Item.create, item.modify, item.reload', function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
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
