var _      = require('lodash');
var assert = require('assert');

exports.cfg = {
  tableName: 'azureEntityTests',
  azure: {
    accountId: process.env.AZURE_ACCOUNT_ID,
    accessKey: process.env.AZURE_ACCOUNT_KEY,
  },
};

if (!exports.cfg.azure.accountId || !exports.cfg.azure.accessKey) {
  console.error('set $AZURE_ACCOUNT_ID and $AZURE_ACCOUNT_KEY to a testing Azure storage account.');
  process.exit(1);
}

exports.contextualSuites = function(name, contexts, suiteFunc) {
  _.forEach(contexts, function(ctx) {
    var options = ctx.options;
    if (typeof options === 'function') {
      options = options();
    }
    suite(name + ' (' + ctx.context + ')', function() {
      suiteFunc.bind(this)(ctx.context, options);
    });
  });
};

exports.makeContexts = function(Item, setupOptions) {
  return [
    {
      context: 'Azure',
      options: function() {
        return {
          Item: Item.setup(_.defaults({}, setupOptions, {
            credentials:  exports.cfg.azure,
            tableName:    exports.cfg.tableName,
          })),
        };
      },
    }, {
      context: 'In-Memory',
      options: function() {
        return {
          Item: Item.setup(_.defaults({}, setupOptions, {
            tableName: 'items',
            credentials: 'inMemory',
          })),
        };
      },
    },
  ];
};

class MockMonitor {
  constructor() {
    this.counts = {};
    this.measures = {};
  }

  count(key) {
    this.counts[key] = (this.counts[key] || 0) + 1;
  }

  measure(key, val) {
    assert(typeof val === 'number', 'Measurement value must be a number');
    this.measures[key] = (this.measures[key] || []).concat(val);
  }
}

exports.MockMonitor = MockMonitor;
