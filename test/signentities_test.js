var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var debug   = require('debug')('test:entity:signEntities');
var helper  = require('./helper');

helper.contextualSuites('Entity (signEntities)', [
  {
    context: 'Azure',
    options: {
      credentials:  helper.cfg.azure,
      tableName:    helper.cfg.tableName,
    },
  }, {
    context: 'In-Memory',
    options: {
      tableName: 'items',
      credentials: 'inMemory',
    },
  },
], function(context, options) {

  var ItemV1;
  test('ItemV1 = Entity.configure', function() {
    ItemV1 = subject.configure({
      version:          1,
      signEntities:     true,
      partitionKey:     subject.keys.StringKey('id'),
      rowKey:           subject.keys.StringKey('name'),
      properties: {
        id:             subject.types.String,
        name:           subject.types.String,
        count:          subject.types.Number,
      },
    });
  });

  var Item;
  test('Item = ItemV1.setup', function() {
    Item = ItemV1.setup(_.defaults({}, options, {
      signingKey:   'no-way-you-can-guess-this',
    }));

    return Item.ensureTable();
  });

  test('ItemV1.setup (requires signingKey)', function() {
    try {
      ItemV1.setup(options);
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  var id = slugid.v4();

  test('Item.create', function() {
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1,
    });
  });

  test('Item.load, item.modify, item.reload()', function() {
    return Item.load({
      id:     id,
      name:   'my-test-item',
    }).then(function(item) {
      assert(item.count === 1);
      return item.modify(function(item) {
        item.count += 1;
      });
    }).then(function(item) {
      assert(item.count === 2);
      return Item.load({
        id:     id,
        name:   'my-test-item',
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
      name:   'my-test-item',
    }).then(function() {
      assert(false, 'Expected an error');
    }, function(err) {
      assert(err.code === 'ResourceNotFound');
    });
  });

  test('Item.load (invalid signature)', function() {
    var BadKeyItem = ItemV1.setup(_.defaults({}, options, {
      signingKey:   'wrong-secret',
    }));
    return BadKeyItem.load({
      id:     id,
      name:   'my-test-item',
    }).then(function() {
      assert(false, 'Expected a signature error');
    }, function(err) {
      assert(err, 'Expected a signature error');
    });
  });

  var ItemV2;
  test('ItemV2 = ItemV1.configure (signEntities: false)', function() {
    ItemV2 = ItemV1.configure({
      version:          2,
      signEntities:     false,
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
  });

  var Item2;
  test('Item2 = ItemV2.setup', function() {
    Item2 = ItemV2.setup(_.defaults({}, options, {
      signingKey:   'no-way-you-can-guess-this',
    }));
  });

  test('ItemV2.setup (requires signingKey)', function() {
    try {
      ItemV2.setup(options);
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error!');
  });

  test('Item2.load (w. migrate)', function() {
    return Item2.load({
      id:     id,
      name:   'my-test-item',
    }).then(function(item) {
      assert(item.count === 2);
      assert(item.reason === 'no-reason');
    });
  });

  test('Item2.load (invalid signature)', function() {
    var BadKeyItem2 = ItemV2.setup(_.defaults({}, options, {
      signingKey:   'wrong-secret',
    }));
    return BadKeyItem2.load({
      id:     id,
      name:   'my-test-item',
    }).then(function() {
      assert(false, 'Expected a signature error');
    }, function(err) {
      assert(err, 'Expected a signature error');
    });
  });

  test('Item2.load, item.modify, item.reload()', function() {
    return Item2.load({
      id:     id,
      name:   'my-test-item',
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
        name:   'my-test-item',
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

  test('ItemV2.configure (signEntities must be explicit)', function() {
    try {
      ItemV2.configure({
        version:          3,
        properties: {
          id:             subject.types.String,
          name:           subject.types.String,
          count:          subject.types.Number,
        },
        migrate: function(item) {
          return {
            id:           item.id,
            name:         item.name,
            count:        item.count,
          };
        },
      });
    } catch (err) {
      return; // Expected error
    }
    assert(false, 'Expected an error, that signEntities wasn\'t explicit');
  });

  var ItemV3;
  test('ItemV3 = ItemV2.configure', function() {
    ItemV3 = ItemV2.configure({
      version:          3,
      signEntities:     true,
      properties: {
        id:             subject.types.String,
        name:           subject.types.String,
        count:          subject.types.Number,
      },
      migrate: function(item) {
        return {
          id:           item.id,
          name:         item.name,
          count:        item.count,
        };
      },
    });
  });

  var Item3;
  test('Item3 = ItemV3.setup', function() {
    Item3 = ItemV3.setup(_.defaults({}, options, {
      signingKey:   'no-way-you-can-guess-this',
    }));
  });

  test('Item3.load, item.modify, item.reload()', function() {
    return Item3.load({
      id:     id,
      name:   'my-test-item',
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
        name:   'my-test-item',
      });
    }).then(function(item) {
      assert(item.count === 4);
      return item.reload().then(function() {
        assert(item.count === 4);
      });
    });
  });

  test('Item3.load (invalid signature)', function() {
    var BadKeyItem3 = ItemV3.setup(_.defaults({}, options, {
      signingKey:   'wrong-secret',
    }));
    return BadKeyItem3.load({
      id:     id,
      name:   'my-test-item',
    }).then(function() {
      assert(false, 'Expected a signature error');
    }, function(err) {
      assert(err, 'Expected a signature error');
    });
  });
});
