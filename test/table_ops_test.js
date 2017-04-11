var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var debug   = require('debug')('test:entity:create_load');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    count:          subject.types.Number
  }
});

helper.contextualSuites("Entity (modify)", [
  {
    context: "Azure",
    options: function() {
      return {
        Item: Item.setup({
          credentials:  helper.cfg.azure,
          // randomize table names, as azure takes a while to delete a table
          table:        helper.cfg.tableName + Math.floor(Math.random() * 20)
        })
      };
    }
  }, {
    context: "In-Memory",
    options: function() {
      return {
        Item: Item.setup({
          account:  "inMemory",
          table:    'items',
          credentials: null,
        })
      };
    }
  }
], function(context, options) {
  var Item = options.Item;

  var cleanup = function() {
    // try to remove the table, ignoring errors (usually 404)
    return Item.removeTable().catch(function(err) { });
  };
  suiteSetup(cleanup);
  suiteTeardown(cleanup);

  test("Item.ensureTable", function() {
    return Item.ensureTable();
  });

  test("Item.ensureTable (again)", function() {
    return Item.ensureTable();
  });

  test("Item.removeTable", function() {
    return Item.removeTable();
  });

  test("Item.removeTable (again, should error)", function() {
    return Item.removeTable().then(function() {
      assert(false, "Expected an error");
    }, function(err) {
      assert(err.code === 'ResourceNotFound');
      assert(err.statusCode === 404, "Expected 404");
    });
  });
});
