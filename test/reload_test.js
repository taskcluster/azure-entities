var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    count:          subject.types.Number,
  },
});

helper.contextualSuites('Entity (reload)', helper.makeContexts(Item),
  function(context, options) {
    var Item = options.Item;

    setup(function() {
      return Item.ensureTable();
    });

    test('Item.create, item.reload', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
      }).then(function(item) {
        return item.reload();
      });
    });

    test('Item.create, item.modify, item.reload', function() {
      var id = slugid.v4();
      return Item.create({
        id:     id,
        name:   'my-test-item',
        count:  1,
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          assert(itemA !== itemB);
          return itemB.modify(function() {
            this.count += 1;
          });
        }).then(function() {
          assert(itemA.count === 1);
          return itemA.reload();
        }).then(function(updated) {
          assert(updated);
          assert(itemA.count === 2);
        }).then(function() {
          return itemA.reload();
        }).then(function(updated) {
          assert(!updated);
          assert(itemA.count === 2);
        });
      });
    });
  });
