var util            = require('util');
var assert          = require('assert');
var _               = require('lodash');
var debug           = require('debug')('base:entity:keys');

/** Base class for all operators */
var Op = function(operator, operand) {
  this.operator = op;
  this.operand  = operand;
};

/** Construct filter for given property with type */
Op.prototype.filter = function(type, property) {
  throw new Error("Not implementing in abstract class");
};

/******************** Ordering Relations ********************/

// Ordering relations
var ORDER_RELATIONS = [
  'gt', 'ge',
  'lt', 'le'
];

/** Class for ordering operators */
var OrderOp = function(op, operand) {
  assert(ORDER_RELATIONS.indexOf(op) !== -1,        "Invalidate operator!");
  assert(operand !== undefined,                     "operand is required");
  Op.call(this, op, operand);
};

util.inherits(OrderOp, Op);

OrderOp.prototype.filter = function(type, property) {
  if (!type.isOrdered) {
    throw new Error("Type for '" + property + "' does not support the " +
                    "operator: '" + this.operator + "'");
  }
  // Serialize
  var target = {};
  type.serialize(target, this.operand);
  // Convert to filters
  var filters = _.map(target, function(value, key) {
    return  [key, this.operator, value].join(' ');
  }, this);
  // For ordered data types there should only be one key/value pair
  assert(filters.length === 1, "isOrdered should only be supported by types " +
                               "serialized to a single key/value pair");
  return filters[0];
};

/******************** Equivalence Relations ********************/

// Equivalence relations
var EQUIVALENCE_RELATIONS = [
  'eq', 'ne',
];

/** Class for simple equivalence operators */
var EquivOp = function(op, operand) {
  assert(EQUIVALENCE_RELATIONS.indexOf(op) !== -1,  "Invalidate operator!");
  assert(operand !== undefined,                     "operand is required");
  Op.call(this, op, operand);
};
util.inherits(EquivOp, Op);

EquivOp.prototype.filter = function(type, property) {
  if (!type.isComparable) {
    throw new Error("Type for '" + property + "' does not support the " +
                    "operator: '" + this.operator + "'");
  }
  // Serialize
  var target = {};
  type.serialize(target, this.operand);
  // Convert to filters
  var filters = _.map(target, function(value, key) {
    return  [key, this.operator, value].join(' ');
  }, this);

  return filters.join(' and ');
};

/******************** Short Hands ********************/

// Short hand for operators
ORDER_RELATIONS.forEach(function(op) {
  Op[op] = function(operand) {
    return new OrderOp(op, operand);
  };
});
EQUIVALENCE_RELATIONS.forEach(function(op) {
  Op[op] = function(operand) {
    return new EquivOp(op, operand);
  };
});

// Human readable short hand for operators
Op.equal                = Op['==']  = Op.eq;
Op.notEqual             = Op['!=']  = Op.ne;
Op.greaterThan          = Op['>']   = Op.gt;
Op.greaterThanOrEqual   = Op['>=']  = Op.ge;
Op.lessThan             = Op['<']   = Op.lt;
Op.lessThanOrEqual      = Op['<=']  = Op.le;

// Export Op with all auxiliary functions
module.exports = Op;