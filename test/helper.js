var _      = require('lodash');
var config = require('typed-env-config');

exports.cfg = config();

exports.contextualSuites = function(name, contexts, suiteFunc) {
  _.forEach(contexts, function(ctx) {
    var options = ctx.options();
    suite(name + " (" + ctx.context + ")", function() { suiteFunc(ctx.context, options) });
  });
};
