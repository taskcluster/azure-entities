var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var helper  = require('./helper');

helper.contextualSuites("Entity (create/load/modify DataTypes)", [
  {
    context: 'Entity.types.String',
    options: {
      type: subject.types.String,
      sample1: "Hello World",
      sample2: "Hello World Again"
    }
  },
  {
    context: 'Entity.types.Boolean',
    options: {
      type: subject.types.Boolean,
      sample1: false,
      sample2: true
    }
  },
  {
    context: 'Entity.types.Number (float)',
    options: {
      type: subject.types.Number,
      sample1: 42.3,
      sample2: 56.7
    }
  },
  {
    context: 'Entity.types.Number (large)',
    options: {
      type: subject.types.Number,
      sample1: 12147483648,
      sample2: 13147483648
    }
  },
  {
    context: 'Entity.types.Number (int)',
    options: {
      type: subject.types.Number,
      sample1: 45,
      sample2: 1256
    }
  },
  {
    context: 'Entity.types.PositiveInteger',
    options: {
      type: subject.types.Number,
      sample1: 455,
      sample2: 125236
    }
  },
  {
    context: 'Entity.types.Date',
    options: {
      type: subject.types.Date,
      sample1: new Date(),
      sample2: new Date('2015-09-01T03:47:24.883Z')
    }
  },
  {
    context: 'Entity.types.UUID',
    options: {
      type: subject.types.UUID,
      sample1: 'f47ac10b-58cc-4372-a567-0e02b2c3d479', // v4 uuid
      sample2: '37175f00-505c-11e5-ad72-69c56eeb1d01'  // v1 uuid
    }
  },
  {
    context: 'Entity.types.SlugId',
    options: {
      type: subject.types.SlugId,
      sample1: 'nvItOmAyRiOvSSWCAHkobQ',
      sample2: 'NgmMmc_oQZ-dC4nPzWI1Ug'
    }
  },
  {
    context: 'Entity.types.JSON',
    options: {
      type: subject.types.JSON,
      sample1: {subobject: {number: 42}, array: [1,2,3,4, "string"]},
      sample2: {subobject: {number: 51}, array: [1,2,3,4,5, "string"]}
    }
  },
  {
    context: 'Entity.types.Schema',
    options: {
      type: subject.types.Schema({
        type: 'object', required: ['subobject', 'array'],
      }),
      sample1: {subobject: {number: 42}, array: [1,2,3,4, "string"]},
      sample2: {subobject: {number: 51}, array: [1,2,3,4,5, "string"]},
    }
  },
  {
    context: 'Entity.types.Blob',
    options: {
      type: subject.types.Blob,
      sample1: crypto.randomBytes(10 * 1000),
      sample2: crypto.randomBytes(100 * 1000)
    }
  },
  {
    context: 'Entity.types.Text',
    options: {
      type: subject.types.Text,
      sample1: "Hello World\n could be a very long string",
      sample2: crypto.randomBytes(100 * 1000).toString('base64')
    }
  },
  // SlugIdArray cannot be tested with _.isEqual, we also have separate tests for
  // this EntityType.
  {
    context: 'Entity.types.EncryptedJSON',
    options: {
      type: subject.types.EncryptedJSON,
      sample1: {subobject: {number: 42}, array: [1,2,3,4, "string"]},
      sample2: {subobject: {number: 51}, array: [1,2,3,4,5, "string"]},
      encryptedTestOnly: true
    }
  },
  {
    context: 'Entity.types.EncryptedSchema',
    options: {
      type: subject.types.EncryptedSchema({
        type: 'object', required: ['subobject', 'array'],
      }),
      sample1: {subobject: {number: 42}, array: [1,2,3,4, "string"]},
      sample2: {subobject: {number: 51}, array: [1,2,3,4,5, "string"]},
      encryptedTestOnly: true
    }
  },
  {
    context: 'Entity.types.EncryptedText',
    options: {
      type: subject.types.EncryptedText,
      sample1: "Hello World\n could be a very long string",
      sample2: crypto.randomBytes(100 * 1000).toString('base64'),
      encryptedTestOnly: true
    }
  },
  {
    context: 'Entity.types.EncryptedBlob',
    options: {
      type: subject.types.EncryptedBlob,
      sample1: crypto.randomBytes(10 * 1000),
      sample2: crypto.randomBytes(100 * 1000),
      encryptedTestOnly: true
    }
  },
], function(name, typeOptions) {
  var type = typeOptions.type;
  var sample1 = typeOptions.sample1;
  var sample2 = typeOptions.sample2;
  var encryptedTestOnly = typeOptions.encryptedTestOnly;

  assert(!_.isEqual(sample1, sample2), "Samples should not be equal!");

  helper.contextualSuites('', [
    {
      context: "Azure",
      options: {
        credentials:  helper.cfg.azure,
        table:        helper.cfg.tableName
      },
    }, {
      context: "In-Memory",
      options: {
        account:   "inMemory",
        table:    "items",
        credentials: null,
      }
    }
  ], function(context, options) {
    setup(function() {
      var Item = subject.configure({
        version:          1,
        partitionKey:     subject.keys.ConstantKey('key1'),
        rowKey:           subject.keys.ConstantKey('key2'),
        properties: {
          id:             subject.types.String,
        }
      }).setup(options);
      Item.ensureTable();
    });

    if (!encryptedTestOnly) {
      test('raw datatype', function() {
        var Item = subject.configure({
          version:          1,
          partitionKey:     subject.keys.StringKey('id'),
          rowKey:           subject.keys.StringKey('name'),
          properties: {
            id:             subject.types.String,
            name:           subject.types.String,
            data:           type
          }
        }).setup(options);

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
            assert(_.isEqual(itemA.data, sample1));
            assert(_.isEqual(itemA.data, itemB.data));
            assert(itemA._etag === itemB._etag);
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
            assert(_.isEqual(itemA.data, sample2));
            assert(_.isEqual(itemA.data, itemB.data));
            assert(itemA._etag === itemB._etag);
            return itemB.modify(function(item) {
              item.data = sample1;
            });
          }).then(function(itemB) {
            assert(_.isEqual(itemA.data, sample2));
            return itemA.reload().then(function() {
              assert(_.isEqual(itemA.data, sample1));
              assert(_.isEqual(itemA.data, itemB.data));
              assert(itemA._etag === itemB._etag);
            }).then(function() {
              // Try noop edit
              var etag = itemA._etag;
              return itemA.modify(function(item) {
                item.data = sample1; // This is the value it already has
              }).then(function() {
                assert(itemA._etag === etag);
              });
            }).then(function() {
              // Try parallel edit
              var count = 0;
              var noop = 0;
              return Promise.all([
                itemA.modify(function(item) {
                  count++;
                  if (_.isEqual(item.data, sample2)) {
                    noop++;
                  }
                  item.data = sample2;
                }),
                itemB.modify(function(item) {
                  count++;
                  if (_.isEqual(item.data, sample2)) {
                    noop++;
                  }
                  item.data = sample2;
                })
              ]).then(function() {
                assert(count === 3, "Expected 3 edits, 2 initial + 1 conflict");
                assert(noop === 1, "Expected 1 noop edit");
                assert(_.isEqual(itemA.data, sample2));
                assert(_.isEqual(itemB.data, sample2));
                assert(_.isEqual(itemA.data, itemB.data));
                // Check that etags match, otherwise we might have updated even when not needed
                assert(itemA._etag);
                assert(itemB._etag);
                assert(itemA._etag === itemB._etag);
              });
            });
          });
        });
      });

      test('signEntities', function() {
        var Item = subject.configure({
          version:          1,
          partitionKey:     subject.keys.StringKey('id'),
          rowKey:           subject.keys.ConstantKey('signing-test-item'),
          signEntities:     true,
          properties: {
            id:             subject.types.String,
            data:           type
          }
        }).setup(_.defaults({}, options, {
          signingKey:   'my-super-secret'
        }));
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

      test('signEntities detect invalid key', function() {
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
        var Item1 = ItemClass.setup(_.defaults({}, options, {
          signingKey:   'my-super-secret'
        }));
        var Item2 = ItemClass.setup(_.defaults({}, options, {
          signingKey:   'my-super-wrong-secret'
        }));
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

    test('w. EncryptedBlob', function() {
      var Item = subject.configure({
        version:          1,
        partitionKey:     subject.keys.StringKey('id'),
        rowKey:           subject.keys.ConstantKey('my-signing-test-item'),
        properties: {
          id:             subject.types.String,
          blob:           subject.types.EncryptedBlob,
          data:           type
        }
      }).setup(_.defaults({}, options, {
        cryptoKey:    'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w='
      }));

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

    test('w. EncryptedBlob + signEntities', function() {
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
      }).setup(_.defaults({}, options, {
        signingKey:   'my-super-secret',
        cryptoKey:    'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w='
      }));

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
  });
});
