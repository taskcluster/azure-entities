var util            = require('util');
var assert          = require('assert');
var _               = require('lodash');
var debug           = require('debug')('base:entity:keys');

// Create dictionary of keys defined and exported
var keys = module.exports = {};

/**
 * Encode string-key, to escape characters for Azure Table Storage and replace
 * empty strings with a single '~', so that empty strings can be allowed.
 */
var encodeStringKey = function(str) {
  // Check for empty string
  if (str === "") {
    return "~";
  }
  // 1. URL encode
  // 2. URL encode all tilde (replace ~ with %7E)
  // 3. Replace % with tilde for Azure compatibility
  return encodeURIComponent(str).replace(/~/g, '%7E').replace(/%/g, '~');
};

/** Decode string-key (opposite of encodeStringKey) */
var decodeStringKey = function(key) {
  // Check for empty string
  if (key === "~") {
    return "";
  }
  // 1. Replace tilde with % to get URL encoded string
  // 2. URL decode (this handle step 1 and 2 from encoding process)
  return decodeURIComponent(key.replace(/~/g, '%'));
};

/******************** String Key ********************/

/** Create StringKey builder */
keys.StringKey = function(key) {
  return function(mapping) {
    return new StringKey(mapping, key);
  };
};


/** Construct a StringKey */
var StringKey = function(mapping, key) {
  // Set key
  this.key = key;

  // Set key type
  assert(mapping[this.key], "key '" + key + "' is not defined in mapping");
  this.type = mapping[this.key];

  // Set covers
  this.covers = [key];
};

/** Construct exact key if possible */
StringKey.prototype.exact = function(properties) {
  // Get value
  var value = properties[this.key];
  // Check that value was given
  assert(value !== undefined, "Unable to create key from properties");
  // Return exact key
  return encodeStringKey(this.type.string(value));
};


/******************** Composite Key ********************/

//TODO: Write a key that can consist of multiple strings

/******************** Hash Key ********************/

//TODO: Write a key that consists of the hash of other keys, sha512 will do

