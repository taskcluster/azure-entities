var util            = require('util');
var assert          = require('assert');
var _               = require('lodash');
var debug           = require('debug')('base:entity:types');

// Create dictionary of types defined and exported
var types = module.exports = {};


/******************** Base Type ********************/

/** Base class for all Entity serializable data types */
types.BaseType = function(property) {
  this.property = property;
};

/** Serialize value to target for property */
types.BaseType.prototype.serialize = function(target, value) {
  throw new Error("Not implemented");
};

/** Compare the two values (deep comparison if necessary) */
types.BaseType.prototype.equal = function(value1, value2) {
  // Compare using serialize(), this works because serialize(), must be
  // implemented, but it might not be the cheapest implementation
  var target1 = {},
      target2 = {};
  this.serialize(target1, value1);
  this.serialize(target2, value2);
  return _.isEqual(target1, target2);
};

/** Get a string representation for key generation (optional) */
types.BaseType.prototype.string = function(value) {
  throw new Error("Operation is not support for this data type");
};

/** Get a string or buffer representation for hash-key generation (optional) */
types.BaseType.prototype.hash = function(value) {
  return this.string(value);
};

/** Deserialize value for property from source */
types.BaseType.prototype.deserialize = function(source) {
  throw new Error("Not implemented");
};


/******************** String Type ********************/

/** String Entity type */
types.String = function(property) {
  types.BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(types.String, types.BaseType);

types.String.prototype.serialize = function(target, value) {
  assert(typeof(value) === 'string', "Serializing property '%s' expected a " +
         "string got: %j", this.property, value);
  target[this.property] = value;
};

types.String.prototype.string = function(value) {
  assert(typeof(value) === 'string', "string(): property '%s' expected a " +
         "string, instead got: %j", this.property, value);
  return value;
};

types.String.prototype.deserialize = function(source) {
  var value = source[this.property];
  assert(typeof(value) === 'string', "Loading property '%s' expected " +
                                     "string for %j", this.property, source);
  return value;
};


/******************** Number Type ********************/

/** Number Entity type */
types.Number = function(property) {
  types.BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(types.Number, types.BaseType);

types.Number.prototype.serialize = function(target, value) {
  assert(typeof(value) === 'number', "Serializing property '%s' expected a " +
         "number got: %j", this.property, value);
  target[this.property] = value;
};

types.Number.prototype.string = function(value) {
  assert(typeof(value) === 'number', "string(): property '%s' expected a " +
         "number, instead got: %j", this.property, value);
  return '' + value;  // Convert to string
};

types.Number.prototype.deserialize = function(source) {
  var value = source[this.property];
  assert(typeof(value) === 'number', "Loading property '%s' expected " +
                                     "number for %j", this.property, source);
  return value;
};

/******************** JSON Type ********************/

//TODO: Support for JSON objects, encoded in utf-8 binary buffers

/******************** Date Type ********************/

//TODO: Support for Date objects stored as date object

/******************** Text Type ********************/

//TODO: Support for longer text, encoded in utf-8 (as buffer)

/******************** Blob Type ********************/

//TODO: Support for binry blobs node.js Buffer

/******************** SlugId Type ********************/

//TODO: Wrap a slugid and encode as uuid

/******************** SlugIdSet Type ********************/

//TODO: Wrap a slugid-set object and encode slugids as binary buffer


