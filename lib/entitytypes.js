var util            = require('util');
var assert          = require('assert');
var _               = require('lodash');
var debug           = require('debug')('base:entity:types');
var slugid          = require('slugid');
var stringify       = require('json-stable-stringify');

/******************** Base Type ********************/

/** Base class for all Entity serializable data types */
var BaseType = function(property) {
  this.property = property;
};

/** Serialize value to target for property */
BaseType.prototype.serialize = function(target, value) {
  throw new Error("Not implemented");
};

/** Compare the two values (deep comparison if necessary) */
BaseType.prototype.equal = function(value1, value2) {
  // Compare using serialize(), this works because serialize(), must be
  // implemented, but it might not be the cheapest implementation
  var target1 = {},
      target2 = {};
  this.serialize(target1, value1);
  this.serialize(target2, value2);
  return _.isEqual(target1, target2);
};

/** Constructor a fairly deep clone of this item */
BaseType.prototype.clone = function(value) {
  var virtualTarget = {};
  this.serialize(virtualTarget, value);
  return this.deserialize(virtualTarget);
};

/** Get a string representation for key generation (optional) */
BaseType.prototype.string = function(value) {
  throw new Error("Operation is not support for this data type");
};

/** Get a string or buffer representation for hash-key generation (optional) */
BaseType.prototype.hash = function(value) {
  return this.string(value);
};

/** Deserialize value for property from source */
BaseType.prototype.deserialize = function(source) {
  throw new Error("Not implemented");
};

// Export BaseType
exports.BaseType = BaseType;

/******************** Value Type ********************/

/** Base class Value Entity types */
var ValueType = function(property) {
  BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(ValueType, BaseType);

/** Validate the type of the value */
ValueType.prototype.validate = function(value) {
  throw new Error("Not implemented");
};

ValueType.prototype.serialize = function(target, value) {
  this.validate(value);
  target[this.property] = value;
};

ValueType.prototype.equal = function(value1, value2) {
  return value1 === value2;
};

ValueType.prototype.clone = function(value) {
  return value;
};

ValueType.prototype.string = function(value) {
  this.validate(value);
  return value;
};

ValueType.prototype.deserialize = function(source) {
  var value = source[this.property];
  this.validate(value);
  return value;
};

// Export ValueType
exports.ValueType = ValueType;

/******************** String Type ********************/

/** String Entity type */
var StringType = function(property) {
  ValueType.apply(this, arguments);
};

// Inherit from ValueType
util.inherits(StringType, ValueType);

StringType.prototype.validate = function(value) {
  assert(typeof(value) === 'string',
         "StringType '%s' expected a string got: %j", this.property, value);
};

// Export StringType as String
exports.String = StringType;


/******************** Number Type ********************/

/** String Entity type */
var NumberType = function(property) {
  ValueType.apply(this, arguments);
};

// Inherit from ValueType
util.inherits(NumberType, ValueType);

NumberType.prototype.validate = function(value) {
  assert(typeof(value) === 'number',
         "NumberType '%s' expected a number got: %j", this.property, value);
};

NumberType.prototype.string = function(value) {
  this.validate(value);
  return value.toString();
};

// Export NumberType as Number
exports.Number = NumberType;

/******************** Date Type ********************/

/** Date Entity type */
var DateType = function(property) {
  BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(DateType, BaseType);

DateType.prototype.validate = function(value) {
  assert(value instanceof Date, "DateType '%s' expected a date got: %j",
         this.property, value);
};

DateType.prototype.serialize = function(target, value) {
  this.validate(value);
  target[this.property] = new Date(value);
};

DateType.prototype.equal = function(value1, value2) {
  this.validate(value1);
  this.validate(value2);
  return value1.getTime() === value2.getTime();
};

DateType.prototype.clone = function(value) {
  this.validate(value);
  return new Date(value);
};

DateType.prototype.string = function(value) {
  this.validate(value);
  return value.toJSON();
};

DateType.prototype.deserialize = function(source) {
  var value = source[this.property];
  this.validate(value);
  return value;
};

// Export DateType as Date
exports.Date = DateType;


/******************** UUID Type ********************/

/** UUID Entity type */
var UUIDType = function(property) {
  ValueType.apply(this, arguments);
};

// Inherit from ValueType
util.inherits(UUIDType, ValueType);

var _uuidExpr = /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/i;

UUIDType.prototype.validate = function(value) {
  assert(typeof(value) === 'string' && _uuidExpr.test(value),
         "UUIDType '%s' expected a uuid got: %j", this.property, value);
};

UUIDType.prototype.equal = function(value1, value2) {
  return value1.toLowerCase() === value2.toLowerCase();
};

UUIDType.prototype.string = function(value) {
  return value.toLowerCase();
};

// Export UUIDType as UUID
exports.UUID = UUIDType;


/******************** SlugId Type ********************/

/** SlugId Entity type */
var SlugIdType = function(property) {
  ValueType.apply(this, arguments);
};

// Inherit from ValueType
util.inherits(SlugIdType, ValueType);

var _slugIdExpr = /^[a-z0-9_-]{22}$/i;

SlugIdType.prototype.validate = function(value) {
  assert(typeof(value) === 'string' && _slugIdExpr.test(value),
         "SlugIdType '%s' expected a slugid got: %j", this.property, value);
};

SlugIdType.prototype.serialize = function(target, value) {
  this.validate(value);
  target[this.property] = slugid.decode(value);
};

SlugIdType.prototype.deserialize = function(source) {
  var value = slugid.encode(source[this.property]);
  return value;
};

// Export SlugIdType as SlugId
exports.SlugId = SlugIdType;

/******************** SlugIdSet Type ********************/

//TODO: Wrap a slugid-set object and encode slugids as binary buffer


/******************** JSON Type ********************/

//TODO: Support for JSON objects, encoded in utf-8 binary buffers

/** JSON Entity type */
var JSONType = function(property) {
  BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(JSONType, BaseType);

JSONType.prototype.serialize = function(target, value) {
  var buf = new Buffer(JSON.stringify(value), 'utf8');
  target[this.property] = buf;
};

JSONType.prototype.equal = function(value1, value2) {
  return _.isEqual(value1, value2);
};

JSONType.prototype.clone = function(value) {
  return _.cloneDeep(value);
};

JSONType.prototype.hash = function(value) {
  return stringify(value);
};

JSONType.prototype.deserialize = function(source) {
  return JSON.parse(source[this.property]);
};

// Export JSONType as JSON
exports.JSON = JSONType;


/******************** Text Type ********************/

//TODO: Support for longer text, encoded in utf-8 (as buffer)

/******************** Blob Type ********************/

/** Blob Entity type */
var BlobType = function(property) {
  BaseType.apply(this, arguments);
};

// Inherit from BaseType
util.inherits(BlobType, BaseType);

BlobType.prototype.validate = function(value) {
  assert(Buffer.isBuffer(value),
         "BlobType '%s' expected a Buffer got: %j", value);
};

BlobType.prototype.serialize = function(target, value) {
  this.validate(value);
  target[this.property] = value;
};

BlobType.prototype.equal = function(value1, value2) {
  this.validate(value1);
  this.validate(value2);
  if (value1 === value2) {
    return true;
  }
  if (value1.length !== value2.length) {
    return false;
  }
  var n = value1.length;
  for (var i = 0; i < n; i++) {
    if (value1[i] !== value2[i]) {
      return false;
    }
  }
  return true;
};

BlobType.prototype.clone = function(value) {
  this.validate(value);
  return new Buffer(value);
};

BlobType.prototype.hash = function(value) {
  return value;
};

BlobType.prototype.deserialize = function(source) {
  return source[this.property];
};

// Export BlobType as Blob
exports.Blob = BlobType;

