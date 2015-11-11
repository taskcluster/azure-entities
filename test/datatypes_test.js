var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var helper  = require('./helper');

suite("Entity (create/load/modify DataTypes)", function() {

  var testType = function(name, type, sample1, sample2, encryptedTestOnly) {
    assert(!_.isEqual(sample1, sample2), "Samples should not be equal!");
    if (!encryptedTestOnly) {
      test(name, function() {
        var Item = subject.configure({
          version:          1,
          partitionKey:     subject.keys.StringKey('id'),
          rowKey:           subject.keys.StringKey('name'),
          properties: {
            id:             subject.types.String,
            name:           subject.types.String,
            data:           type
          }
        }).setup({
          credentials:  helper.cfg.azure,
          table:        helper.cfg.tableName
        });

        var id = slugid.v4();
        return Item.create({
          id:     id,
          name:   'my-test-item',
          data:   sample1
        }).then(function(itemA) {
          return Item.load({
            id:     id,
            name:   'my-test-item'
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample1));
            return itemB;
          });
        }).then(function(item) {
          return item.modify(function(item) {
            item.data = sample2;
          });
        }).then(function(itemA) {
          return Item.load({
            id:     id,
            name:   'my-test-item'
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample2));
          });
        });
      });

      test(name + ' (signEntities)', function() {
        var Item = subject.configure({
          version:          1,
          partitionKey:     subject.keys.StringKey('id'),
          rowKey:           subject.keys.ConstantKey('signing-test-item'),
          signEntities:     true,
          properties: {
            id:             subject.types.String,
            data:           type
          }
        }).setup({
          credentials:  helper.cfg.azure,
          table:        helper.cfg.tableName,
          signingKey:   'my-super-secret'
        });
        var id = slugid.v4();
        return Item.create({
          id:     id,
          data:   sample1
        }).then(function(itemA) {
          return Item.load({
            id:     id
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample1));
            return itemB;
          });
        }).then(function(item) {
          return item.modify(function(item) {
            item.data = sample2;
          });
        }).then(function(itemA) {
          return Item.load({
            id:     id
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample2));
          });
        });
      });

      test(name + ' (signEntities detect invalid key)', function() {
        var ItemClass = subject.configure({
          version:          1,
          partitionKey:     subject.keys.StringKey('id'),
          rowKey:           subject.keys.ConstantKey('signing-test-item'),
          signEntities:     true,
          properties: {
            id:             subject.types.String,
            data:           type
          }
        })
        var Item1 = ItemClass.setup({
          credentials:  helper.cfg.azure,
          table:        helper.cfg.tableName,
          signingKey:   'my-super-secret'
        });
        var Item2 = ItemClass.setup({
          credentials:  helper.cfg.azure,
          table:        helper.cfg.tableName,
          signingKey:   'my-super-wrong-secret'
        });
        var id = slugid.v4();
        return Item1.create({
          id:     id,
          data:   sample1
        }).then(function(itemA) {
          return Item2.load({id: id}).then(function() {
            assert(false, "Expected an error");
          }, function() {
            return Item1.load({id: id});
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample1));
            return itemB;
          });
        }).then(function(item) {
          return item.modify(function(item) {
            item.data = sample2;
          });
        }).then(function(itemA) {
          return Item1.load({
            id:     id
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, itemB.data));
            assert(_.isEqual(itemA.data, sample2));
          });
        }).then(function() {
          return Item2.load({id: id}).then(function() {
            assert(false, "Expected an error");
          }, function() {
            // Ignore expected error
          });
        });
      });
    }

    test(name + ' (w. EncryptedBlob)', function() {
      var Item = subject.configure({
        version:          1,
        partitionKey:     subject.keys.StringKey('id'),
        rowKey:           subject.keys.ConstantKey('my-signing-test-item'),
        properties: {
          id:             subject.types.String,
          blob:           subject.types.EncryptedBlob,
          data:           type
        }
      }).setup({
        credentials:  helper.cfg.azure,
        table:        helper.cfg.tableName,
        cryptoKey:    'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w='
      });

      var id = slugid.v4();
      return Item.create({
        id:     id,
        blob:   new Buffer([1,2,3,4,5,6,7,8]),
        data:   sample1
      }).then(function(itemA) {
        return Item.load({
          id:     id
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, sample1));
          return itemB;
        });
      }).then(function(item) {
        return item.modify(function(item) {
          item.data = sample2;
        });
      }).then(function(itemA) {
        return Item.load({
          id:     id
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, sample2));
        });
      });
    });

    test(name + ' (w. EncryptedBlob + signEntities)', function() {
      var Item = subject.configure({
        version:          1,
        partitionKey:     subject.keys.StringKey('id'),
        rowKey:           subject.keys.ConstantKey('my-signing-test-item'),
        signEntities:     true,
        properties: {
          id:             subject.types.String,
          blob:           subject.types.EncryptedBlob,
          data:           type
        }
      }).setup({
        credentials:  helper.cfg.azure,
        table:        helper.cfg.tableName,
        signingKey:   'my-super-secret',
        cryptoKey:    'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w='
      });

      var id = slugid.v4();
      return Item.create({
        id:     id,
        blob:   new Buffer([1,2,3,4,5,6,7,8]),
        data:   sample1
      }).then(function(itemA) {
        return Item.load({
          id:     id
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, sample1));
          return itemB;
        });
      }).then(function(item) {
        return item.modify(function(item) {
          item.data = sample2;
        });
      }).then(function(itemA) {
        return Item.load({
          id:     id
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, sample2));
        });
      });
    });
  };

  testType(
    'Entity.types.String',
    subject.types.String,
    "Hello World",
    "Hello World Again"
  );
  testType(
    'Entity.types.Number (float)',
    subject.types.Number,
    42.3,
    56.7
  );
  testType(
    'Entity.types.Number (large)',
    subject.types.Number,
    12147483648,
    13147483648
  );
  testType(
    'Entity.types.Number (int)',
    subject.types.Number,
    45,
    1256
  );
  testType(
    'Entity.types.Date',
    subject.types.Date,
    new Date(),
    new Date('2015-09-01T03:47:24.883Z')
  );
  testType(
    'Entity.types.UUID',
    subject.types.UUID,
    'f47ac10b-58cc-4372-a567-0e02b2c3d479', // v4 uuid
    '37175f00-505c-11e5-ad72-69c56eeb1d01'  // v1 uuid
  );
  testType(
    'Entity.types.SlugId',
    subject.types.SlugId,
    'nvItOmAyRiOvSSWCAHkobQ',
    'NgmMmc_oQZ-dC4nPzWI1Ug'
  );
  testType(
    'Entity.types.JSON',
    subject.types.JSON,
    {
      subobject: {number: 42},
      array: [1,2,3,4, "string"]
    }, {
      subobject: {number: 51},
      array: [1,2,3,4,5, "string"]
    }
  );
  testType(
    'Entity.types.Blob',
    subject.types.Blob,
    crypto.randomBytes(10 * 1000),
    crypto.randomBytes(100 * 1000)
  );
  testType(
    'Entity.types.Text',
    subject.types.Text,
    "Hello World\n could be a very long string",
    crypto.randomBytes(100 * 1000).toString('base64')
  );
  // SlugIdArray cannot be tested with _.isEqual, we also have separate tests for
  // this EntityType.
  testType(
    'Entity.types.EncryptedJSON',
    subject.types.EncryptedJSON,
    {
      subobject: {number: 42},
      array: [1,2,3,4, "string"]
    }, {
      subobject: {number: 51},
      array: [1,2,3,4,5, "string"]
    },
    true
  );
  testType(
    'Entity.types.EncryptedText',
    subject.types.EncryptedText,
    "Hello World\n could be a very long string",
    crypto.randomBytes(100 * 1000).toString('base64'),
    true
  );
  testType(
    'Entity.types.EncryptedBlob',
    subject.types.EncryptedBlob,
    crypto.randomBytes(10 * 1000),
    crypto.randomBytes(100 * 1000),
    true
  );
});
