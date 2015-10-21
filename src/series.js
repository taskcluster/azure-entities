'use strict';

var stats = require('taskcluster-lib-stats');
var Series = stats.Series;

/** Statistics from Azure table operations */
exports.AzureTableOperations = new Series({
  name: 'AzureTableOperations',
  columns: {
    component: stats.types.String,
    process: stats.types.String,
    duration: stats.types.Number,
    table: stats.types.String,
    method: stats.types.String,
    error: stats.types.String
  }
});
