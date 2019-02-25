var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var debug   = require('debug')('test:entity:create_load');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    count:          subject.types.Number,
    time:           subject.types.Date,
  },
});

helper.contextualSuites('Entity (modify)', helper.makeContexts(Item),
  function(context, options) {
    var Item = options.Item;

    setup(function() {
      return Item.ensureTable();
    });

    test('Item.create, Item.modify, Item.load', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 1);
        return item.modify(function() {
          this.count += 1;
        }).then(function(item2) {
          assert(item instanceof Item);
          assert(item.id === id);
          assert(item.count === 2);
          assert(item2 instanceof Item);
          assert(item2.id === id);
          assert(item2.count === 2);
          assert(item === item2);
        });
      }).then(function() {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        });
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 2);
      });
    });

    test('Item.create, Item.modify, throw error', function() {
      var id = slugid.v4();
      var err = new Error('Testing that errors in modify works');
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function(item) {
        return item.modify(function() {
          throw err;
        });
      }).then(function() {
        assert(false, 'Expected an error');
      }, function(err2) {
        assert(err === err2, 'Expected the error I threw!');
      });
    });

    test('Item.modify a deleted itedm', function() {
      var id = slugid.v4();
      var deletedItem;
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function(item) {
        deletedItem = item;
        return Item.remove({id: id, name: 'my-test-item'});
      }).then(function() {
        return deletedItem.modify(function(item) {
          item.count += 1;
        });
      }).then(function() {
        assert(false, 'Expected an error');
      }, function(err) {
        assert(err.code === 'ResourceNotFound', 'Expected ResourceNotFound');
        assert(err.statusCode == 404, 'Expected 404');
      });
    });

    test('Item.create, Item.modify (first argument), Item.load', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 1);
        return item.modify(function(item) {
          item.count += 1;
        });
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 2);
        return Item.load({
          id:     id,
          name:   'my-test-item',
        });
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 2);
      });
    });

    test('Item.modify (concurrent)', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          return Promise.all([
            itemA.modify(function() {
              this.count += 1;
            }),
            itemB.modify(function() {
              this.count += 1;
            }),
          ]);
        });
      }).then(function() {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        });
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 3);
      });
    });

    test('Item.modify (concurrent 5)', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
        time:   new Date(),
      }).then(function() {
        var promisedItems = [];
        for (var i = 0; i < 5; i++) {
          promisedItems.push(Item.load({
            id:     id,
            name:   'my-test-item',
          }));
        }
        return Promise.all(promisedItems);
      }).then(function(items) {
        return Promise.all(items.map(function(item) {
          return item.modify(function() {
            this.count += 1;
            // finish the modify functions in random time
            var res = new Promise(function(resolve, reject) {
              setTimeout(resolve, Math.floor(Math.random() * 100));
            });
            return res;
          });
        }));
      }).then(function() {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        });
      }).then(function(item) {
        assert(item instanceof Item);
        assert(item.id === id);
        assert(item.count === 6);
      });
    });
  });
