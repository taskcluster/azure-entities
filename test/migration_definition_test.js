var subject = require("../lib/entity")
suite("Entity (migration validate-keys)", function() {
  var base    = require("taskcluster-base")
  var assert  = require('assert');

  test("Can migrate", function() {
    subject.configure({
      version:        1,
      partitionKey:   subject.keys.StringKey('pk'),
      rowKey:         subject.keys.StringKey('rk'),
      properties: {
        pk:           subject.types.String,
        rk:           subject.types.Number
      }
    }).configure({
      version:        2,
      properties: {
        pk:           subject.types.String,
        rk:           subject.types.Number,
        value:        subject.types.String
      },
      migrate: function(item) {
        item.value = "none";
        return item;
      }
    });
  });

  test("Can migrate (with context)", function() {
    subject.configure({
      version:        1,
      partitionKey:   subject.keys.StringKey('pk'),
      rowKey:         subject.keys.StringKey('rk'),
      properties: {
        pk:           subject.types.String,
        rk:           subject.types.Number
      },
      context:        ['myKey', 'anotherKey']
    }).configure({
      version:        2,
      properties: {
        pk:           subject.types.String,
        rk:           subject.types.Number,
        value:        subject.types.String
      },
      context:        ['anotherKey'], // Should overwrite old context
      migrate: function(item) {
        item.value = "none";
        return item;
      }
    });
  });

  test("Can't define key with missing property", function() {
    assert.throws(function() {
      subject.configure({
        version:        1,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          value:        subject.types.String,
          rk:           subject.types.Number
        }
      });
    }, "Expected an error");
  });

  test("Can't migrate key properties (redefinition)", function() {
    assert.throws(function() {
      subject.configure({
        version:        1,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        }
      }).configure({
        version:        2,
        partitionKey:   subject.keys.StringKey('value'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number,
          value:        subject.types.String
        },
        migrate: function(item) {
          item.value = "none";
          return item;
        }
      });
    }, "Expected an error");
  });

  test("Can't migrate key properties (rename)", function() {
    assert.throws(function() {
      subject.configure({
        version:        1,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        }
      }).configure({
        version:        2,
        properties: {
          pk2:          subject.types.String,
          rk:           subject.types.Number
        },
        migrate: function(item) {
          return {
            pk2:    item.pk,
            rk:     item.rk
          };
        }
      });
    }, "Expected an error");
  });

  test("Can't migrate key properties (types)", function() {
    assert.throws(function() {
      subject.configure({
        version:        1,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        }
      }).configure({
        version:        2,
        properties: {
          pk:           subject.types.Number,
          rk:           subject.types.Number
        },
        migrate: function(item) {
          return {
            pk:     parseInt(item.pk),
            rk:     item.rk
          };
        }
      });
    }, "Expected an error");
  });

  test("Can't start with version 2", function() {
    assert.throws(function() {
      subject.configure({
        version:        2,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        }
      });
    }, "Expected an error");
  });

  test("Can't migrate with version + 2", function() {
    assert.throws(function() {
      subject.configure({
        version:        1,
        partitionKey:   subject.keys.StringKey('pk'),
        rowKey:         subject.keys.StringKey('rk'),
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        }
      }).configure({
        version:        3,
        properties: {
          pk:           subject.types.String,
          rk:           subject.types.Number
        },
        migrate: function(item) {
          return {
            pk:     parseInt(item.pk),
            rk:     item.rk
          };
        }
      });
    }, "Expected an error");
  });
});