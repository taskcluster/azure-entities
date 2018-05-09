var subject   = require('../lib/entity');
var helper    = require('./helper');
var assert    = require('assert');
var slugid    = require('slugid');
var _         = require('lodash');
var Promise   = require('promise');
var debug     = require('debug')('test:entity:create_load');
var _monitor = require('taskcluster-lib-monitor');

var ItemV1 = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    count:          subject.types.Number,
  },
});

suite('Monitoring Integration', function() {
  var monitor = null;
  var Item = null;
  var id = null;
  suiteSetup(async function() {
    monitor = await _monitor({
      project: 'azure-entities',
      credentials: {},
      mock: true,
    });
    Item = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      monitor:      monitor,
    }),
    id = slugid.v4();

    await Item.ensureTable();
    await Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
  });

  test('Item.load writes stats', function() {
    assert(_.keys(monitor.counts).length === 2, 'Should only have counts from create and insert.');
    return Item.load({
      id:     id,
      name:   'my-test-item',
    }).then(function(item) {
      assert(_.keys(monitor.counts).length >= 3, 'Should have more stats now!');
      assert(item.count === 1);
    });
  });
});
