var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var debug   = require('debug')('test:entity:encryptedProps');
var crypto  = require('crypto');
var azure   = require('fast-azure-storage');
var helper  = require('./helper');

suite('Entity (encrypted properties)', function() {

  var ENCRYPTION_KEY = 'gRT3AtCOwtZOP4xI6ESB4vPC3unDXqKq8VolcnDISPE=';

  var ItemV1;
  test('ItemV1 = Entity.configure', function() {
    ItemV1 = subject.configure({
      version:          1,
      partitionKey:     subject.keys.StringKey('id'),
      rowKey:           subject.keys.ConstantKey('enc-props-test'),
      properties: {
        id:             subject.types.String,
        count:          subject.types.EncryptedJSON,
      },
    });
  });

  var Item;
  test('Item = ItemV1.setup', function() {
    Item = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    ENCRYPTION_KEY,
    });
  });

  test('ItemV1.setup (requires cryptoKey)', function() {
    try {
      ItemV1.setup({
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  test('ItemV1.setup (cryptoKey < 32 bytes doesn\'t work)', function() {
    try {
      ItemV1.setup({
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
        cryptoKey:    crypto.randomBytes(31).toString('base64'),
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  test('ItemV1.setup (cryptoKey > 32 bytes doesn\'t work)', function() {
    try {
      ItemV1.setup({
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
        cryptoKey:    crypto.randomBytes(33).toString('base64'),
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  test('ItemV1.setup (requires cryptoKey in base64)', function() {
    try {
      ItemV1.setup({
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
        // Notice: ! below
        cryptoKey:    'CNcj2aOozdo7Pn+HEkAIixwninIwKnbYc6JPS9mNxZ!=',
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  var id = slugid.v4();

  test('Item.create', function() {
    return Item.create({
      id:     id,
      count:  1,
    });
  });

  test('Item.load, item.modify, item.reload()', function() {
    return Item.load({
      id:     id,
    }).then(function(item) {
      assert(item.count === 1);
      return item.modify(function(item) {
        item.count += 1;
      });
    }).then(function(item) {
      assert(item.count === 2);
      return Item.load({
        id:     id,
      });
    }).then(function(item) {
      assert(item.count === 2);
      return item.reload().then(function() {
        assert(item.count === 2);
      });
    });
  });

  test('Item.load (missing)', function() {
    return Item.load({
      id:     slugid.v4(),
    }).then(function() {
      assert(false, 'Expected an error');
    }, function(err) {
      assert(err.code === 'ResourceNotFound');
    });
  });

  test('Item.load (invalid cryptoKey)', function() {
    var BadKeyItem = ItemV1.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    crypto.randomBytes(32).toString('base64'),
    });
    return BadKeyItem.load({
      id:     id,
    }).then(function() {
      assert(false, 'Expected a decryption error');
    }, function(err) {
      assert(err, 'Expected a decryption error');
    });
  });

  test('check for stable encrypted form', async function() {
    const item = await Item.load({id});

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
        '__buf0_count@odata.type': 'Edm.Binary',
        __bufchunks_count: 1,
        // encrypted version of 1234
        __buf0_count: '+HqJql/AFykC590r8Fkxmhg/2tqsnCOX2eOfbuDKpx8=',
      }, {
        mode: 'merge',
        eTag: '*',
      });

    await item.reload();
    assert.equal(item.count, 1234);

    // restore the expected value of count for the following tests
    await item.modify(function(item) {
      item.count = 2;
    });
  });

  var ItemV2;
  test('ItemV2 = ItemV1.configure (no encryption)', function() {
    ItemV2 = ItemV1.configure({
      version:          2,
      properties: {
        id:             subject.types.String,
        count:          subject.types.Number,
        reason:         subject.types.String,
      },
      migrate: function(item) {
        return {
          id:           item.id,
          count:        item.count,
          reason:       'no-reason',
        };
      },
    });
  });

  var Item2;
  test('Item2 = ItemV2.setup', function() {
    Item2 = ItemV2.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    ENCRYPTION_KEY,
    });
  });

  test('ItemV2.setup (requires cryptoKey)', function() {
    try {
      ItemV2.setup({
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  test('Item2.load (w. migrate)', function() {
    return Item2.load({
      id:     id,
    }).then(function(item) {
      assert(item.count === 2);
      assert(item.reason === 'no-reason');
    });
  });

  test('Item2.load (invalid cryptoKey)', function() {
    var BadKeyItem2 = ItemV2.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    crypto.randomBytes(32).toString('base64'),
    });
    return BadKeyItem2.load({
      id:     id,
    }).then(function() {
      assert(false, 'Expected a decryption error');
    }, function(err) {
      assert(err, 'Expected a decryption error');
    });
  });

  test('Item2.load, item.modify, item.reload()', function() {
    return Item2.load({
      id:     id,
    }).then(function(item) {
      assert(item.count === 2);
      assert(item.reason === 'no-reason');
      return item.modify(function(item) {
        item.count += 1;
        item.reason = 'some-reason';
      });
    }).then(function(item) {
      assert(item.count === 3);
      assert(item.reason === 'some-reason');
      return item.reload().then(function() {
        assert(item.count === 3);
        assert(item.reason === 'some-reason');
      });
    }).then(function() {
      return Item2.load({
        id:     id,
      });
    }).then(function(item) {
      assert(item.count === 3);
      assert(item.reason === 'some-reason');
      return item.reload().then(function() {
        assert(item.count === 3);
        assert(item.reason === 'some-reason');
      });
    });
  });

  var ItemV3;
  test('ItemV3 = ItemV2.configure', function() {
    ItemV3 = ItemV2.configure({
      version:          3,
      properties: {
        id:             subject.types.String,
        count:          subject.types.EncryptedJSON,
      },
      migrate: function(item) {
        return {
          id:           item.id,
          count:        item.count,
        };
      },
    });
  });

  var Item3;
  test('Item3 = ItemV3.setup', function() {
    Item3 = ItemV3.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    ENCRYPTION_KEY,
    });
  });

  test('Item3.load, item.modify, item.reload()', function() {
    return Item3.load({
      id:     id,
    }).then(function(item) {
      assert(item.count === 3);
      assert(item.reason === undefined);
      return item.modify(function(item) {
        item.count += 1;
      });
    }).then(function(item) {
      assert(item.count === 4);
      return item.reload().then(function() {
        assert(item.count === 4);
      });
    }).then(function() {
      return Item3.load({
        id:     id,
      });
    }).then(function(item) {
      assert(item.count === 4);
      return item.reload().then(function() {
        assert(item.count === 4);
      });
    });
  });

  test('Item3.load (invalid cryptoKey)', function() {
    var BadKeyItem3 = ItemV3.setup({
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
      cryptoKey:    crypto.randomBytes(32).toString('base64'),
    });
    return BadKeyItem3.load({
      id:     id,
    }).then(function() {
      assert(false, 'Expected a decryption error');
    }, function(err) {
      assert(err, 'Expected a decryption error');
    });
  });
});
