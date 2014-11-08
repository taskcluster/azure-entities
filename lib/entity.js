var assert          = require('assert');
var util            = require('util');
var slugid          = require('slugid');
var _               = require('lodash');
var Promise         = require('promise');
var debug           = require('debug')('base:entity');
var azureTable      = require('azure-table-node');

/** Base class of all entity */
var Entity = function(entity) {
  // Set __etag
  this.__etag = entity.__etag || null;

  // Create shadow object
  this.__shadow = {};

  // TODO: Fix this
  _.forIn(this.__mapping, function(entry, key) {
    this.__shadow[entry.property] = entry.deserialize(entity[key]);
  }, this);
};

// Built-in type handlers
Entity.types = require('./entitytypes');


/**
 * Configure a subclass of `this` (`Entity` or subclass thereof) with following
 * options:
 * {
 *   // Azure connection details (typically configured at dynamically)
 *   tableName:         "AzureTableName",   // Azure table name
 *   credentials: {
 *     accountName:     "...",              // Azure account name
 *     accountKey:      "...",              // Azure account key
 *   },
 *
 *   // Storage schema details (typically configured statically)
 *   version:           2,                    // Version of the schema
 *   partitionKey:      Entity.HashKey('p1'), // Partition key, can be StringKey
 *   rowKey:            Entity.StringKey('p2', 'p3'), // RowKey...
 *   properties: {
 *     prop1:           Entity.types.Blob,    // Properties and types
 *     prop2:           Entity.types.String,
 *     prop3:           Entity.types.Number,
 *     prop4:           Entity.types.JSON
 *   },
 *   migrate: function(itemV1) {              // Migration function, if not v1
 *     return // transform item from version 1 to version 2
 *   }
 * }
 *
 * When creating a subclass of `Entity` using this method, you must provide all
 * options before you try to initialize instances of the subclass. You may
 * create a subclass hierarchy and provide options one at the time. More details
 * below.
 *
 * When creating a subclass using `configure` all the class properties and
 * class members (read static functions like `Entity.configure`) will also be
 * inherited. So it is possible to do as follows:
 *
 * ```js
 * // Create an abstract key-value pair
 * var AbstractKeyValue = Entity.configure({
 *   version:     1,
 *   partitionKey:    Entity.StringKey('key'),
 *   rowKey:          Entity.ConstantKey('kv-pair'),
 *   properties: {
 *     key:           Entity.types.String,
 *     value:         Entity.types.JSON
 *   }
 * });
 *
 * // Return a pair from the key-value pair
 * AbstractKeyValue.pair = function() {
 *   return [this.key, this.value];
 * };

 * // Create one key-value entity table
 * var KeyValue1 = AbstractKeyValue.configure({
 *   credentials:    {...},
 *   tableName:      "KeyValueTable1"
 * });

 * // Create another key-value entity table
 * var KeyValue1 = AbstractKeyValue.configure({
 *   credentials:    {...},
 *   tableName:      "KeyValueTable2"
 * });
 * ```
 *
 * As illustrated above you can use `configure` to have multiple instantiations
 * of the same Entity configuration. In addition `configure` can also be used
 * define newer revisions of the schema. When doing this, you must base it on
 * the previous version, and you must increment version number by 1 and only 1.
 *
 * ```js
 * // Overwrite the previous definition AbstractKeyValue with a new version
 * AbstractKeyValue = AbstractKeyValue.configure({
 *   version:         2,
 *   partitionKey:    Entity.StringKey('key'),
 *   rowKey:          Entity.ConstantKey('kv-pair'),
 *   properties: {
 *     key:           Entity.types.String,
 *     date:          Entity.types.Date
 *   },
 *   migrate: function(item) {
 *     // Translate from version 1 to version 2
 *     return {
 *       key:      item.key,
 *       date:     new Date(item.value.StringDate)
 *     };
 *   }
 * });
 * ```
 *
 * It's your responsibility that `partitionKey` and `rowKey` will keep
 * returning the same value, otherwise you cannot migrate entities on-the-fly,
 * but must take your application off-line while you upgrade the data schema.
 * Or start submitting data to an additional table, while you're migrating
 * existing data in an off-line process.
 *
 * Typically, `Entity.configure` will be used in a module to create a subclass
 * of Entity with neat auxiliary static class methods and useful members, then
 * this abstract type will again be subclassed and configured with connection
 * credentials and table name. This allows for multiple tables with the same
 * abstract definition, and improves testability by removing configuration
 * from global module scope.
 */
Entity.configure = function(options) {
  // Identify the parent class, that is always `this` so we can use it on
  // subclasses
  var Parent = this;

  // Create a subclass of Parent
  var subClass = function(entity) {
    // Always pass down the entity we're initializing from
    Parent.call(this, entity);
  };
  util.inherits(subClass, Parent);

  // Inherit class methods too (static members in C++)
  _.assign(subClass, Parent);


  // If credentials are provided validate them and add an azure table client
  if (options.credentials) {
    assert(options.credentials,             "Azure credentials must be given");
    assert(options.credentials.accountName, "Missing accountName");
    assert(options.credentials.accountKey ||
           options.credentials.sas,         "Missing accountKey or sas");
    // Add accountUrl, if not already present, there is really no reason to
    // not just compute... That's what the Microsoft libraries does anyways
    var credentials = _.defaults({}, options.credentials, {
      accountUrl:  "https://" + options.credentials.accountName +
                   ".table.core.windows.net/"
    });
    assert(/^https:\/\//.test(credentials.accountUrl),
                                              "Don't use non-HTTPS accountUrl");
    subClass.prototype._azClient = azureTable.createClient(credentials);
  }

  // If tableName is provide validate and add it
  if (options.tableName) {
    assert(typeof(options.tableName) === 'string', "tableName isn't a string");
    subClass.prototype._azTableName = options.tableName;
  }

  // If mapping is given assign it
  if (options.mapping) {
    subClass.prototype.__mapping = normalizeMapping(options.mapping);
    // Define access properties
    _.forIn(subClass.prototype.__mapping, function(entry) {
      if (entry.hidden) {
        return;
      }
      // Define property for accessing underlying shadow object
      Object.defineProperty(subClass.prototype, entry.property, {
        enumerable: true,
        get:        function() {return this.__shadow[entry.property]; }
      });
    });
  }

  // Return subClass
  return subClass;
};


/**
 * Create an entity on azure table with property and mapping.
 * Returns a promise for an instance of `this` (typically an Entity subclass)
 */
Entity.create = function(properties) {
  var Class = this;
  assert(properties,  "Properties is required");
  assert(Class,       "Entity.create must be bound to an Entity subclass");
  assert(Class.prototype._azClient,     "Azure credentials not configured");
  assert(Class.prototype._azTableName,  "Azure tableName not configured");
  assert(Class.prototype.__mapping,     "Property mapping not configured");

  // Return a promise that we inserted the entity
  return new Promise(function(accept, reject) {
    // Construct entity from properties
    var entity = {};
    _.forIn(Class.prototype.__mapping, function(entry, key) {
      entity[key] = entry.serialize(properties[entry.property]);
    });

    // Insert entity
    Class.prototype._azClient.insertEntity(Class.prototype._azTableName,
                                           entity, function(err, etag) {
      // Reject if we have an error
      if (err) {
        debug("Failed to insert entity: %j", entity);
        return reject(err);
      }

      // Add etag to entity
      entity.__etag = etag;

      // Return entity that we inserted
      debug("Inserted entity: %j", entity);
      accept(entity);
    });
  }).then(function(entity) {
    // Construct Entity subclass using Class
    return new Class(entity);
  });
};

/**
 * Load Entity subclass from azure given PartitionKey and RowKey,
 * This method return a promise for the subclass instance.
 */
Entity.load = function(partitionKey, rowKey) {
  var Class = this;
  assert(partitionKey !== undefined &&
         partitionKey !== null,         "PartitionKey is required");
  assert(rowKey !== undefined &&
         rowKey !== null,               "RowKey is required");
  assert(Class,           "Entity.create must be bound to an Entity subclass");
  var client    = Class.prototype._azClient;
  var tableName = Class.prototype._azTableName;
  var mapping   = Class.prototype.__mapping;
  assert(client,    "Azure credentials not configured");
  assert(tableName, "Azure tableName not configured");
  assert(mapping,   "Property mapping not configured");

  // Serialize partitionKey and rowKey
  partitionKey  = mapping.PartitionKey.serialize(partitionKey);
  rowKey        = mapping.RowKey.serialize(rowKey);
  return new Promise(function(accept, reject) {
    client.getEntity(tableName, partitionKey, rowKey, function(err, entity) {
      // Reject if there is an error
      if (err) {
        return reject(err);
      }

      // Accept constructed entity, we'll wrap below, to catch exceptions
      accept(entity);
    });
  }).then(function(entity) {
    // Construct and return Entity subclass using constructor
    return new Class(entity);
  });
};








// Export Entity
module.exports = Entity;
