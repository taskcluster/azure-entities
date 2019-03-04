const sinon     = require('sinon');
const subject   = require('../src/entity');
const helper    = require('./helper');
const assert    = require('assert');
const slugid    = require('slugid');
const _         = require('lodash');
const debug     = require('debug')('test:entity:create_load');

const ItemV1 = subject.configure({
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
  let monitor = null;
  let oldbug = null;
  let newbug = null;

  setup(async function() {
    oldbug = subject.debug;
    newbug = sinon.fake();
    subject.debug = newbug;
    monitor = new helper.MockMonitor();
  });

  teardown(function() {
    subject.debug = oldbug;
  });

  test('Item.load writes stats (report chance)', async function() {
    const Item = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      monitor:      monitor,
      operationReportChance: 1.0,
    });
    const id = slugid.v4();

    await Item.ensureTable();
    await Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
    assert.equal(_.keys(monitor.counts).length, 2, 'Should only have counts from create and insert.');
    const item = await Item.load({
      id:     id,
      name:   'my-test-item',
    });
    assert(_.keys(monitor.counts).length >= 3, 'Should have more stats now!');
    assert.equal(_.keys(monitor.counts)[0], 'createTable.error');
    assert.equal(item.count, 1);
    assert.equal(newbug.callCount, 3);
    assert(newbug.firstCall.args[0].startsWith('TIMING: createTable on azureEntityTests took'));
  });

  test('Item.load writes stats (report threshold)', async function() {
    const Item = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      monitor:      monitor,
      operationReportThreshold: 0.0,
    });
    const id = slugid.v4();

    await Item.ensureTable();
    await Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
    assert.equal(_.keys(monitor.counts).length, 2, 'Should only have counts from create and insert.');
    const item = await Item.load({
      id:     id,
      name:   'my-test-item',
    });
    assert(_.keys(monitor.counts).length >= 3, 'Should have more stats now!');
    assert.equal(_.keys(monitor.counts)[0], 'createTable.error');
    assert.equal(item.count, 1);
    assert.equal(newbug.callCount, 3);
    assert(newbug.firstCall.args[0].startsWith('TIMING: createTable on azureEntityTests took'));
  });

  test('Item.load writes stats (no reports)', async function() {
    const Item = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      monitor:      monitor,
    });
    const id = slugid.v4();

    await Item.ensureTable();
    await Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
    assert.equal(_.keys(monitor.counts).length, 2, 'Should only have counts from create and insert.');
    const item = await Item.load({
      id:     id,
      name:   'my-test-item',
    });
    assert(_.keys(monitor.counts).length >= 3, 'Should have more stats now!');
    assert.equal(_.keys(monitor.counts)[0], 'createTable.error');
    assert.equal(item.count, 1);
    assert.equal(newbug.callCount, 0);
  });

  test('Invalid report chance', function() {
    assert.throws(() => ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      monitor:      monitor,
      operationReportChance: 1.4,
    }), /.*operationReportChance.*/);
  });
});
