var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var debug   = require('debug')('test:entity:hashkey');
var helper  = require('./helper');

suite("Entity (HashKey)", function() {

  var Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.HashKey('id', 'data'),
    rowKey:           subject.keys.HashKey('text1', 'text2'),
    properties: {
      text1:          subject.types.Text,
      text2:          subject.types.String,
      id:             subject.types.SlugId,
      data:           subject.types.JSON
    }
  }).setup({
      credentials:  helper.cfg.azure,
      table:        helper.cfg.tableName
  });

  test("Item.create, HashKey.exact (test against static data)", function() {
    var id = slugid.v4();
    return Item.create({
      id:       id,
      data:     {my: "object", payload: 42},
      text1:    "some text for the key",
      text2:    "another string for the key"
    }).then(function(item) {
      var hash = item.__rowKey.exact(item._properties);
      assert(hash === '8cdcd277cf2ddcb7be572019ef154756' +
                      '86484a3c3eeb4fe3caa5727f0aadd7c9' +
                      '8b873a64a7c54336a3f973e1902d4f1f' +
                      '1dbe7a067943b12b3948a96b4a3acc19');
    });
  });

  test("Item.create, Item.load", function() {
    var id = slugid.v4();
    return Item.create({
      id:       id,
      data:     {my: "object", payload: 42},
      text1:    "some text for the key",
      text2:    "another string for the key"
    }).then(function() {
      return Item.load({
        id:       id,
        data:     {payload: 42, my: "object"},
        text1:    "some text for the key",
        text2:    "another string for the key"
      });
    });
  });

  test("Can't modify key", function() {
    var id = slugid.v4();
    return Item.create({
      id:       id,
      data:     {my: "object", payload: 42},
      text1:    "some text for the key",
      text2:    "another string for the key"
    }).then(function(item) {
      return item.modify(function() {
        this.text1 = "This will never work";
      }).then(function() {
        assert(false, "Expected an error!");
      }, function(err) {
        debug("Catched Expected error")
        assert(err);
      });
    });
  });
});
