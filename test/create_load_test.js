var subject = require('../src/entity');
var helper  = require('./helper');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var debug   = require('debug')('test:entity:create_load');

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

var ItemV2 = ItemV1.configure({
  version:          2,
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    count:          subject.types.Number,
    reason:         subject.types.String,
  },
  migrate: function(item) {
    return {
      id:           item.id,
      name:         item.name,
      count:        item.count,
      reason:       'no-reason',
    };
  },
});

helper.contextualSuites('Entity (create/load)', [
  {
    context: 'Azure',
    options: function() {
      return {
        Item: ItemV1.setup({
          credentials:  helper.cfg.azure,
          tableName:    helper.cfg.tableName,
        }),
        Item2: ItemV2.setup({
          credentials:  helper.cfg.azure,
          tableName:    helper.cfg.tableName,
        }),
      };
    },
  }, {
    context: 'In-Memory',
    options: function() {
      return {
        Item: ItemV1.setup({
          tableName: 'items',
          credentials: 'inMemory',
        }),
        Item2: ItemV2.setup({
          tableName: 'items',
          credentials: 'inMemory',
        }),
      };
    },
  },
], function(context, options) {
  var Item  = options.Item,
    Item2 = options.Item2;
  var id = slugid.v4();

  test('Item.ensureTable', function() {
    return Item.ensureTable();
  });

  test('Item.create', function() {
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
  });

  /*
  // Dirty hack for testing perf when disabling nagle
  test("Item.create 1k items", function() {
    var count = 400;
    var createNext = function() {
      return Item.create({
        id:     slugid.v4(),
        name:   'my-test-item',
        count:  1
      }).then(function() {
        count -= 1;
        if (count > 0) {
          console.log(count);
          return createNext();
        }
      });
    };
    return Promise.all([createNext(), createNext()]);
  }); return; //*/

  test('Item.create (won\'t overwrite)', function() {
    return Item.create({
      id:     id,
      name:   'my-test-item5',
      count:  1,
    }).then(function() {
      return Item.create({
        id:     id,
        name:   'my-test-item5',
        count:  1,
      }).then(function() {
        assert(false, 'Expected error');
      }, function(err) {
        assert(err.code === 'EntityAlreadyExists',
          'Expected EntityAlreadyExists');
        assert(err.statusCode === 409, 'Expected 409');
      });
    });
  });

  test('Item.create (overwriteIfExists)', function() {
    return Item.create({
      id:     id,
      name:   'my-test-item10',
      count:  1,
    }).then(function() {
      return Item.create({
        id:     id,
        name:   'my-test-item10',
        count:  2,
      }, true);
    }).then(function() {
      return Item.load({
        id:     id,
        name:   'my-test-item10',
      }).then(function(item) {
        assert(item.count === 2);
      });
    });
  });

  test('Item.load', function() {
    return Item.load({
      id:     id,
      name:   'my-test-item',
    }).then(function(item) {
      assert(item.count === 1);
    });
  });

  test('Item.load (missing)', function() {
    return Item.load({
      id:     slugid.v4(),
      name:   'my-test-item',
    }).then(function() {
      assert(false, 'Expected an error');
    }, function(err) {
      assert(err.code === 'ResourceNotFound');
      assert(err.statusCode === 404, 'Expected 404');
    });
  });

  test('Item.load (ignoreIfNotExists)', function() {
    return Item.load({
      id:     slugid.v4(),
      name:   'my-test-item',
    }, true).then(function(item) {
      assert(item === null, 'Expected an null to be returned');
    });
  });

  test('Item2.load', function() {
    return Item2.load({
      id:     id,
      name:   'my-test-item',
    }).then(function(item) {
      assert(item.count === 1);
      assert(item.reason === 'no-reason');
    });
  });
});
