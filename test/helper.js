var _      = require('lodash');
var config = require('typed-env-config');

exports.cfg = config();

exports.contextualSuites = function(name, contexts, suiteFunc) {
  _.forEach(contexts, function(ctx) {
    var options = ctx.options;
    if (typeof options === "function") {
      options = options();
    }
    suite(name + " (" + ctx.context + ")", function() {
      suiteFunc.bind(this)(ctx.context, options);
    });
  });
};

exports.makeContexts = function(Item, setupOptions) {
  return [
    {
      context: "Azure",
      options: function() {
        return {
          Item: Item.setup(_.defaults({}, setupOptions, {
            credentials:  exports.cfg.azure,
            table:        exports.cfg.tableName
          }))
        };
      }
    }, {
      context: "In-Memory",
      options: function() {
        return {
          Item: Item.setup(_.defaults({}, setupOptions, {
            account: "inMemory",
            table:   'items'
          }))
        };
      }
    }
  ];
}
