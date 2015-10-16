var subject = require("../lib/entity")
suite("Entity (ConstantKey)", function() {
  var assert  = require('assert');
  var slugid  = require('slugid');
  var _       = require('lodash');
  var Promise = require('promise');
  var base    = require("taskcluster-base")
  var crypto  = require('crypto');
  var debug   = require('debug')('base:test:entity:compositekey');

  var helper  = require('./helper');
  var cfg = helper.loadConfig();

  test("Item.create, Item.load (without properties)", function() {
    var Item = subject.configure({
      version:          1,
      partitionKey:     subject.keys.ConstantKey(slugid.v4()),
      rowKey:           subject.keys.ConstantKey(slugid.v4()),
      properties: {
        data:           subject.types.Number
      }
    }).setup({
      credentials:  cfg.get('azure'),
      table:        cfg.get('azureTestTableName')
    });

    return Item.create({
      data:     42,
    }).then(function(itemA) {
      return Item.load().then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(itemB.data === 42);
      });
    });
  });

  test("Item.create, Item.load (combined with CompositeKey)", function() {
    var Item = subject.configure({
      version:          1,
      partitionKey:     subject.keys.CompositeKey('taskId', 'runId'),
      rowKey:           subject.keys.ConstantKey("task-info"),
      properties: {
        taskId:         subject.types.SlugId,
        runId:          subject.types.Number,
        data:           subject.types.Number
      }
    }).setup({
      credentials:  cfg.get('azure'),
      table:        cfg.get('azureTestTableName')
    });

    var id = slugid.v4();
    return Item.create({
      taskId:   id,
      runId:    0,
      data:     42
    }).then(function(itemA) {
      return Item.load({
        taskId:   id,
        runId:    0
      }).then(function(itemB) {
        assert(itemA.data === itemB.data);
        assert(itemB.data === 42);
      });
    });
  });
});
