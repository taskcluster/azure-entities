var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var stats   = require("taskcluster-lib-stats");
var debug   = require('debug')('test:entity:query');
var helper  = require('./helper');

helper.contextualSuites("Entity (query)", [
  {
    context: "Azure",
    options: {
      credentials:  helper.cfg.azure,
      table:        helper.cfg.tableName,
    },
  }, {
    context: "In-Memory",
    options: {
      inMemory: true,
      table: "items",
    }
  },
], function(context, options) {

  var Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.StringKey('id'),
    rowKey:           subject.keys.StringKey('name'),
    properties: {
      id:             subject.types.String,
      name:           subject.types.String,
      count:          subject.types.Number,
      tag:            subject.types.String,
      time:           subject.types.Date
    }
  }).setup(_.defaults({}, options, {
    component:    '"taskcluster-base"-test',
    process:      'mocha'
  }));

  setup(function() {
    return Item.ensureTable();
  });

  var id = slugid.v4();
  before(function() {
    return Item.ensureTable().then(function() {
      return Promise.all([
        Item.create({
          id:     id,
          name:   'item1',
          count:  1,
          tag:    'tag1',
          time:   new Date(0)
        }),
        Item.create({
          id:     id,
          name:   'item2',
          count:  2,
          tag:    'tag2',
          time:   new Date(1)
        }),
        Item.create({
          id:     id,
          name:   'item3',
          count:  3,
          tag:    'tag1',   // same tag as item1
          time:   new Date(1000000000000)
        })
      ]);
    });
  });

  test("Query a partition", function() {
    return Item.query({id: id}).then(function(data) {
      assert(data.entries.length === 3);
      var sum = 0;
      data.entries.forEach(function(item) {
        sum += item.count;
      });
      assert(sum === 6);
    });
  });

  test("Query a partition (with Entity.op.equal)", function() {
    return Item.query({
      id:     subject.op.equal(id)
    }).then(function(data) {
      assert(data.entries.length === 3);
      var sum = 0;
      data.entries.forEach(function(item) {
        sum += item.count;
      });
      assert(sum === 6);
    });
  });

  test("Can't query without partition-key", function() {
    return Promise.resolve().then(function() {
     return Item.query({
        name:   'item1',
        count:  1,
        tag:    'tag1'
      });
    }).then(function() {
      assert(false, "Expected an error");
    }, function(err) {
      debug("Caught expected error: %j", err)
    });
  });

  test("Query a partition (with limit 2)", function() {
    return Item.query({id: id}, {
      limit:      2
    }).then(function(data) {
      assert(data.entries.length === 2);
      assert(data.continuation);

      // Fetch next
      return Item.query({id: id}, {
        limit:          2,
        continuation:   data.continuation
      }).then(function(data) {
        assert(data.entries.length === 1);
        assert(!data.continuation);
      });
    });
  });

  test("Query a partition (with handler)", function() {
    var sum = 0;
    return Item.query({id: id}, {
      handler:      function(item) { sum += item.count; }
    }).then(function() {
      assert(sum === 6);
    });
  });

  test("Query a partition (with async handler)", function() {
    var sum = 0;
    return Item.query({id: id}, {
      handler:      function(item) {
        return new Promise(function(accept) {
          setTimeout(function() {
            sum += item.count;
            accept();
          }, 150);
        });
      }
    }).then(function() {
      assert(sum === 6);
    });
  });

  test("Query a partition (with handler and limit 2)", function() {
    var sum = 0;
    return Item.query({id: id}, {
      limit:        2,
      handler:      function(item) { sum += item.count; }
    }).then(function() {
      assert(sum === 6);
    });
  });

  test("Query a partition (with async handler and limit 2)", function() {
    var sum = 0;
    return Item.query({id: id}, {
      limit:        2,
      handler:      function(item) {
        return new Promise(function(accept) {
          setTimeout(function() {
            sum += item.count;
            accept();
          }, 150);
        });
      }
    }).then(function() {
      assert(sum === 6);
    });
  });

  test("Filter by tag", function() {
    var sum = 0;
    return Item.query({
      id:     id,
      tag:    'tag1'
    }).then(function(data) {
      assert(data.entries.length === 2);
      data.entries.forEach(function(item) {
        assert(item.tag === 'tag1');
      });
    });
  });

  test("Filter by tag (with handler)", function() {
    var sum = 0;
    return Item.query({
      id:     id,
      tag:    'tag1'
    }, {
      handler:      function(item) { sum += item.count; }
    }).then(function() {
      assert(sum === 4);
    });
  });

  test("Filter by time < Date(1)", function() {
    var sum = 0;
    return Item.query({
      id:       id,
      time:     subject.op.lessThan(new Date(1))
    }).then(function(data) {
      assert(data.entries.length === 1);
      assert(data.entries[0].name == 'item1');
    });
  });

  test("Filter by time < Date(100)", function() {
    var sum = 0;
    return Item.query({
      id:       id,
      time:     subject.op.lessThan(new Date(100))
    }).then(function(data) {
      assert(data.entries.length === 2);
    });
  });

  test("Filter by time > Date(100)", function() {
    var sum = 0;
    return Item.query({
      id:       id,
      time:     subject.op.greaterThan(new Date(100))
    }).then(function(data) {
      assert(data.entries.length === 1);
      assert(data.entries[0].name == 'item3');
    });
  });

  test("Filter by count < 3", function() {
    return Item.query({
      id:       id,
      count:    subject.op.lessThan(3)
    }).then(function(data) {
      assert(data.entries.length === 2);
      data.entries.forEach(function(item) {
        assert(item.count < 3);
      });
    });
  });

  test("Query for specific row (matchRow: exact)", function() {
    return Item.query({
      id:     id,
      name:   'item2'
    }, {
      matchRow:   'exact'
    }).then(function(data) {
      assert(data.entries.length === 1);
      data.entries.forEach(function(item) {
        assert(item.tag === 'tag2');
      });
    });
  });

  test("Can't use matchRow: exact without row-key", function() {
    return Promise.resolve().then(function() {
      return Item.query({
        id:     id,
      }, {
        matchRow:   'exact'
      });
    }).then(function() {
      assert(false, "Expected an error");
    }, function(err) {
      debug("Caught expected error: %j", err);
    });
  });
});
