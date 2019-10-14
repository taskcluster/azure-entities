var _      = require('lodash');
var assert = require('assert');
const taskcluster = require('taskcluster-client');

const credentials = {};
exports.cfg = {
  tableName: 'azureEntityTests',
  azure: credentials,
};

suiteSetup(async () => {
  credentials.accountId = process.env.AZURE_ACCOUNT;
  credentials.accessKey = process.env.AZURE_ACCOUNT_KEY;

  if (credentials.accountId && credentials.accessKey) {
    return;
  }

  // load credentials from the secret if running in CI
  if (process.env.TASKCLUSTER_PROXY_URL) {
    console.log('loading credentials from secret via TASKCLUSTER_PROXY_URL');
    const client = new taskcluster.Secrets({rootUrl: process.env.TASKCLUSTER_PROXY_URL});
    const res = await client.get('project/taskcluster/testing/azure');
    console.log(res.secret);
    credentials.accountId = res.secret.AZURE_ACCOUNT;
    credentials.accessKey = res.secret.AZURE_ACCOUNT_KEY;
    return;
  }

  console.error('set $AZURE_ACCOUNT and $AZURE_ACCOUNT_KEY to a testing Azure storage account.');
  process.exit(1);
});

exports.contextualSuites = function(name, contexts, suiteFunc) {
  _.forEach(contexts, function(ctx) {
    var options = ctx.options;
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
