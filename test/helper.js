var _      = require('lodash');
var config = require('typed-env-config');

exports.cfg = config();

exports.contextualSuites = function(name, contexts, suiteFunc) {
  _.forEach(contexts, function(ctx) {
    var options = ctx.options;
    if (typeof options === "function") {
      options = options();
    }
    suite(name + " (" + ctx.context + ")", function() { suiteFunc(ctx.context, options) });
  });
};

exports.makeContexts = function(Item) {
  return [
    {
      context: "Azure",
      options: function() {
        return {
          Item: Item.setup({
            credentials:  exports.cfg.azure,
            table:        exports.cfg.tableName
          })
        };
      }
    }, {
      context: "In-Memory",
      options: function() {
        return {
          Item: Item.setup({
            inMemory: true,
            table:    'items'
          })
        };
      }
    }
  ];
}
