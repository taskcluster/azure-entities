var subject = require('../src/entity');
var assert  = require('assert');
var azure   = require('fast-azure-storage');
var slugid  = require('slugid');
var _       = require('lodash');
var crypto  = require('crypto');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    data:           subject.types.EncryptedBlob,
  },
});

helper.contextualSuites('Entity (EncryptedBlobType)', helper.makeContexts(Item, {
  cryptoKey:    'CNcj2aOozdo7Pn+HEkAIixwninIwKnbYc6JPS9mNxZk=',
}), function(context, options) {
  let Item;
  suiteSetup(function() {
    Item = options().Item;
  });

  setup(function() {
    return Item.ensureTable();
  });

  var compareBuffers = function(b1, b2) {
    assert(Buffer.isBuffer(b1));
    assert(Buffer.isBuffer(b2));
    if (b1.length !== b2.length) {
      return false;
    }
    var n = b1.length;
    for (var i = 0; i < n; i++) {
      if (b1[i] !== b2[i]) {
        return false;
      }
    }
    return true;
  };

  test('small blob', function() {
    var id  = slugid.v4();
    var buf = Buffer.from([0, 1, 2, 3]);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   buf,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(compareBuffers(itemA.data, itemB.data));
      });
    });
  });

  if (context === 'Azure') {
    test('check for stable encrypted form', async function() {
      const id = slugid.v4();
      const name = 'my-test-item';
      const data = Buffer.from([9, 9, 9, 9]); // *not* 1, 2, 3, 4
      const item = await Item.create({id, name, data});

      // overwrite the `data` column with an encrypted value captured from a
      // successful run.  This test then ensures that no changes causes
      // existing rows to no longer decrypt correctly.
      const table = new azure.Table({
        accountId: helper.cfg.azure.accountId,
        accessKey: helper.cfg.azure.accessKey,
      });
      await table.updateEntity(
        helper.cfg.tableName, {
          PartitionKey: item._partitionKey,
          RowKey: item._rowKey,
          '__buf0_data@odata.type': 'Edm.Binary',
          __bufchunks_data: 1,
          // encrypted version of [0, 1, 2, 3]
          __buf0_data: '4uNlCrg5nvXMRpXC9Hz87of+M5KjrA69qFgh2/s3OfY=',
        }, {
          mode: 'merge',
          eTag: '*',
        });

      await item.reload();
      assert.deepEqual(item.data, Buffer.from([0, 1, 2, 3]));
    });
  }

  test('large blob (64k)', function() {
    var id  = slugid.v4();
    var buf = crypto.pseudoRandomBytes(64 * 1024);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   buf,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(compareBuffers(itemA.data, itemB.data));
      });
    });
  });

  test('large blob (128k)', function() {
    var id  = slugid.v4();
    var buf = crypto.pseudoRandomBytes(128 * 1024);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   buf,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(compareBuffers(itemA.data, itemB.data));
      });
    });
  });

  test('large blob (256k - 32)', function() {
    var id  = slugid.v4();
    var buf = crypto.pseudoRandomBytes(256 * 1024 - 32);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   buf,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(compareBuffers(itemA.data, itemB.data));
      });
    });
  });
});
