var subject = require('../lib/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var debug   = require('debug')('test:entity:compositekey');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.CompositeKey('id', 'data'),
  rowKey:           subject.keys.CompositeKey('text1', 'text2'),
  properties: {
    text1:          subject.types.String,
    text2:          subject.types.String,
    id:             subject.types.SlugId,
    data:           subject.types.Number,
  },
});

helper.contextualSuites('Entity (CompositeKey)', helper.makeContexts(Item),
  function(context, options) {
    var Item = options.Item;

    setup(function() {
      return Item.ensureTable();
    });

    test('Item.create, Item.load', function() {
      var id = slugid.v4();
      return Item.create({
        id:       id,
        data:     42,
        text1:    'some text for the key',
        text2:    'another string for the key',
      }).then(function() {
        return Item.load({
          id:       id,
          data:     42,
          text1:    'some text for the key',
          text2:    'another string for the key',
        });
      });
    });

    test('Can\'t modify key', function() {
      var id = slugid.v4();
      return Item.create({
        id:       id,
        data:     42,
        text1:    'some text for the key',
        text2:    'another string for the key',
      }).then(function(item) {
        return item.modify(function() {
          this.text1 = 'This will never work';
        }).then(function() {
          assert(false, 'Expected an error!');
        }, function(err) {
          debug('Catched Expected error');
          assert(err);
        });
      });
    });

    test('Using an empty strings', function() {
      var id = slugid.v4();
      return Item.create({
        id:       id,
        data:     42,
        text1:    '',
        text2:    'another string for the key',
      }).then(function() {
        return Item.load({
          id:       id,
          data:     42,
          text1:    '',
          text2:    'another string for the key',
        });
      });
    });
  });
