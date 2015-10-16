var subject = require("../lib/entity")
suite("Entity (modify)", function() {
  var assert  = require('assert');
  var slugid  = require('slugid');
  var _       = require('lodash');
  var Promise = require('promise');
  var base    = require("taskcluster-base")
  var debug   = require('debug')('base:test:entity:create_load');

  var helper  = require('./helper');
  var cfg = helper.loadConfig();

  var Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.StringKey('id'),
    rowKey:           subject.keys.StringKey('name'),
    properties: {
      id:             subject.types.String,
      name:           subject.types.String,
      count:          subject.types.Number
    }
  }).setup({
    credentials:  cfg.get('azure'),
    table:        cfg.get('azureTestTableName')
  });


  test("Item.create, Item.modify, Item.load", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
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

  test("Item.create, Item.modify, throw error", function() {
    var id = slugid.v4();
    var err = new Error("Testing that errors in modify works");
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
    }).then(function(item) {
      return item.modify(function() {
        throw err;
      });
    }).then(function() {
      assert(false, "Expected an error");
    }, function(err2) {
      assert(err === err2, "Expected the error I threw!");
    });
  });

  test("Item.create, Item.modify (first argument), Item.load", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
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

  test("Item.modify (concurrent)", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
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
          })
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

  test("Item.modify (concurrent 5)", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      count:  1
    }).then(function() {
      var promisedItems = [];
      for(var i = 0; i < 5; i++) {
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
