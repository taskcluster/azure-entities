var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var crypto  = require('crypto');
var azure   = require('fast-azure-storage');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    data:           subject.types.EncryptedText,
  },
});

helper.contextualSuites('Entity (EncryptedTextType)', helper.makeContexts(Item, {
  cryptoKey:    'CNcj2aOozdo7Pn+HEkAIixwninIwKnbYc6JPS9mNxZk=',
}), function(context, options) {
  let Item;
  suiteSetup(function() {
    Item = options().Item;
  });

  setup(function() {
    return Item.ensureTable();
  });

  // Construct a large string
  var randomString = function(kbytes) {
    var s = 'abcefsfcccsrcsdfsdfsfrfdefdwedwiedowijdwoeidnwoifneoifnweodnwoid';
    s += s; // 128
    s += s; // 256
    s += s; // 512
    s += s; // 1024
    var arr = [];
    for (var i = 0; i < kbytes; i++) {
      arr.push(s);
    }
    return arr.join('');
  };

  test('largeString helper', function() {
    var text  = randomString(64);
    assert(text.length === 1024 * 64);
  });

  test('small text', function() {
    var id    = slugid.v4();
    var text  = 'Hello World';
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   text,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(text === itemB.data);
      });
    });
  });

  if (context === 'Azure') {
    test('check for stable encrypted form', async function() {
      const id = slugid.v4();
      const name = 'my-test-item';
      const data = 'not-what-we-want-to-see';
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
          // encrypted version of 'EXPECTED'
          __buf0_data: 'seMOJM+lR6L+dfcHFy4efU9biT9fsNEZrtad2o/fSVY=',
        }, {
          mode: 'merge',
          eTag: '*',
        });

      await item.reload();
      assert.deepEqual(item.data, 'EXPECTED');
    });
  }

  test('large text (64k)', function() {
    var id    = slugid.v4();
    var text  = randomString(64);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   text,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(text === itemB.data);
      });
    });
  });

  test('large text (128k)', function() {
    var id    = slugid.v4();
    var text  = randomString(128);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   text,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(text === itemB.data);
      });
    });
  });

  test('large text (256k - 32)', function() {
    var id    = slugid.v4();
    var text  = randomString(256);
    // Remove 16 to make room for iv
    text = text.substr(0, text.length - 32);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   text,
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item',
      }).then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(text === itemB.data);
      });
    });
  });
});
