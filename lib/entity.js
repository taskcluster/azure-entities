var assert          = require('assert');
var util            = require('util');
var slugid          = require('slugid');
var _               = require('lodash');
var Promise         = require('promise');
var debug           = require('debug')('base:entity');
var azureTable      = require('azure-table-node');

// ** Coding Style **
// To ease reading of this component we recommend the following code guidelines:
//
// Summary:
//    - Use __ prefix for private variables in `Entity`
//    - Variables named `entity` are semi-raw `azure-table-node` types
//    - Variables named `item` are instances of `Entity`
//    - Variables named `properties` are deserialized `entity` objects
//
// Long Version:
//
//
//    * Variables named `entity` are "raw" entities, well raw in the sense that
//      they interface the transport layer provided by `azure-table-node`.
//
//    * Variables named `item` refers instances of `Entity` or instances of
//      subclasses of `Entity`. This is slightly confusing as people using the
//      `Entity` class (of subclasses thereof) are more likely refer to generic
//      instances of their subclasses as `entity` and not `item`.
//      We draw the distinction here because `azure-table-node` is closer to
//      Azure Table Storage which uses the terminology entities. Also subclasses
//      of `Entity` usually has another name, like `Artifact`, so non-generic
//      instances are easily referred to using a variant of that name,
//      like `artifact` as an instance of `Artifact`.
//
//    * Variables named `properties` is usually a mapping from property names
//      to deserialized values.
//
//    * Properties that are private the `Entity` class, should be prefixed `__`,
//      this way subclasses of `Entity` (created with `Entity.configure`) can
//      rely on properties prefixed `_` as being private to them.
//
//    * Try to prevent users from making mistakes. Or doing illegal things, like
//      modifying objects unintentionally without changes being saved.
//
// Okay, that's it for now, happy hacking...


/** List of property names reserved or conflicting with method names */
var RESERVED_PROPERTY_NAMES = [
  // Reserved by Azure Table Storage
  'PartitionKey',
  'RowKey',
  'Timestamp',

  // Reserved for internal use
  'Version',

  // Properties built-in to expose built-in properties
  'version',

  // Methods implemented by `Entity`
  'modify',
  'remove'
];

/**
 * Max number of modify attempts to make when experiencing collisions with
 * optimistic concurrency.
 */
var MAX_MODIFY_ATTEMPTS     = 5;

/**
 * Base class of all entity
 *
 * This constructor will wrap a raw azure-table-node entity.
 */
var Entity = function(entity) {
  assert(entity.PartitionKey, "entity is missing 'PartitionKey'");
  assert(entity.RowKey,       "entity is missing 'RowKey'");
  assert(entity.__etag,       "entity is missing '__etag'");
  assert(entity.Version,      "entity is missing 'Version'");

  // Set __etag
  this.__etag = entity.__etag || null;

  // Deserialize a shadow object from the entity
  this.__properties = this.__deserialize(entity);
};

// Built-in type handlers
Entity.types  = require('./entitytypes');

// Built-in key handlers
Entity.keys   = require('./entitykeys');

// Define properties set in configure
Entity.prototype.__deserialize  = undefined;  // Method to deserialize entities
Entity.prototype.__mapping      = undefined;  // Schema mapping to types
Entity.prototype.__version      = undefined;  // Schema version
Entity.prototype.__partitionKey = undefined;  // PartitionKey builder
Entity.prototype.__rowKey       = undefined;  // RowKey builder

// Define properties set in setup
Entity.prototype.__tableName    = undefined;  // Azure table name
Entity.prototype.__client       = undefined;  // Client from azure-table-node
Entity.prototype.__aux          = undefined;  // Bound denodified client methods

// Define properties set in constructor
Entity.prototype.__properties   = undefined;  // Deserialized shadow object
Entity.prototype.__etag         = undefined;  // Etag of remote entity

/**
 * Create a promise handler that will pass arguments + err to debug()
 * and rethrow err. This is useful as handler for .catch()
 */
var rethrowDebug = function() {
  var args = arguments;
  return function(err) {
    var params = Array.prototype.slice.call(args);
    params.push(err);
    debug.call(debug, params);
    throw err;
  };
};

/**
 * Create a promise handler that will wrap the resulting entity in `Class`.
 * This is useful as handler for .then()
 */
var wrapEntityClass = function (Class) {
  return function(entity) {
    return new Class(entity);
  };
};

/**
 * Configure a subclass of `this` (`Entity` or subclass thereof) with following
 * options:
 * {
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
 * options before you try to call `Entity.setup` and is able to initialize
 * instances of the subclass. You may create a subclass hierarchy and call
 * configure multiple times to allow for additional versions.
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
 *
 * // Return a pair from the key-value pair
 * AbstractKeyValue.pair = function() {
 *   return [this.key, this.date];
 * };

 * // Create one key-value entity table
 * var KeyValue1 = AbstractKeyValue.setup({
 *   credentials:    {...},
 *   tableName:      "KeyValueTable1"
 * });

 * // Create another key-value entity table
 * var KeyValue1 = AbstractKeyValue.setup({
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
 * It's your responsibility that `partitionKey` and `rowKey` will keep
 * returning the same value, otherwise you cannot migrate entities on-the-fly,
 * but must take your application off-line while you upgrade the data schema.
 * Or start submitting data to an additional table, while you're migrating
 * existing data in an off-line process.
 *
 * Typically, `Entity.configure` will be used in a module to create a subclass
 * of Entity with neat auxiliary static class methods and useful members, then
 * this abstract type will again be sub-classed using `setup` with connection
 * credentials and table name. This allows for multiple tables with the same
 * abstract definition, and improves testability by removing configuration
 * from global module scope.
 */
Entity.configure = function(options) {
  assert(options,                                 "options must be given");
  assert(typeof(options.version) === 'number',    "version must be a number");
  assert(typeof(options.properties) === 'object', "properties must be given");
  assert(options.partitionKey,                    "partitionKey is required");
  assert(options.rowKey,                          "rowKey is required");

  // Identify the parent class, that is always `this` so we can use it on
  // subclasses
  var Parent = this;

  // Create a subclass of Parent
  var subClass = function(entity) {
    // Always pass down the entity we're initializing from
    Parent.call(this, entity);
  };
  util.inherits(subClass, Parent);

  // Inherit class methods too (ie. static members)
  _.assign(subClass, Parent);

  // Validate property names
  _.forIn(options.properties, function(Type, property) {
    assert(RESERVED_PROPERTY_NAMES.indexOf(property) === -1,
           "Property name '" + property + "' is reserved");
    assert(!/^__/.test(property),         "Names prefixed '__' is reserved");
    assert(/^[a-zA-Z_-][a-zA-Z0-9_-]*$/.test(property),
           "Property name '" + property + "' is not a proper identifier");
  });

  // Don't allow configure to run after setup, there is no reasons for this.
  // In particular it will give issues access properties when new versions
  // are introduced. Mainly that empty properties will exist.
  assert(
    !subClass.prototype.__tableName &&
    !subClass.prototype.__client,
    "This `Entity` subclass is already setup!"
  );

  // Construct mapping
  var mapping = {};
  _.forIn(options.properties, function(Type, property) {
    mapping[property] = new Type(property);
  });
  subClass.prototype.__mapping = mapping;

  // Construct __partitionKey and __rowKey
  subClass.prototype.__partitionKey = options.partitionKey(mapping);
  subClass.prototype.__rowKey       = options.rowKey(mapping);

  // define __deserialize in two ways
  if (options.version === 1) {
    // If version is 1, we just assert that an deserialize properties
    subClass.prototype.__deserialize = function(entity) {
      assert(entity.Version === 1, "entity.Version isn't 1");
      var properties = {};
      _.forIn(mapping, function(type, property) {
        properties[property] = type.deserialize(entity);
      });
      return properties;
    };
  } else {
    assert(options.migrate instanceof Function,
           "`migrate` must be specified for version > 1");
    // check that version in incremented by 1
    assert(options.version === subClass.prototype.__version + 1,
           "`version` must be incremented by 1 and only 1");
    // if version is > 1, then we remember the deserializer from version - 1
    // if version of the entity we get is < version, then we call the old
    // `deserialize` method (hence, why we keep a reference to it).
    var deserialize = subClass.prototype.__deserialize;
    subClass.prototype.__deserialize = function(entity) {
      // Validate version
      assert(entity.Version <= options.version,
             "entity.Version is greater than configured version!");
      // Migrate, if necessary
      if (entity.Version < options.version) {
        return options.migrate(deserialize(entity));
      }
      // Deserialize properties, if not migrated
      var properties = {};
      _.forIn(mapping, function(type, property) {
        properties[property] = type.deserialize(entity);
      });
      return properties;
    };
  }

  // Set version
  subClass.prototype.__version = options.version;

  // Return subClass
  return subClass;
};


/**
 * Setup a subclass of `this` (`Entity` or subclass thereof) for use, with
 * the following options:
 * {
 *   // Azure connection details
 *   tableName:         "AzureTableName",   // Azure table name
 *   credentials: {
 *     accountName:     "...",              // Azure account name
 *     accountKey:      "...",              // Azure account key
 *   },
 * }
 *
 * Once you have configured properties, version, migration, keys, using
 * `Entity.configure`, you can call `Entity.setup` on your new subclass.
 * This will again create a new subclass that is ready for use, with azure
 * credentials, etc. This new subclass cannot be configured further, nor can
 * `setup` be called again.
 */
Entity.setup = function(options) {
  // Validate options
  assert(options,                         "options must be given");
  assert(options.tableName,               "options.tableName must be given");
  assert(typeof(options.tableName) === 'string', "tableName isn't a string");
  assert(options.credentials,             "Azure credentials must be given");
  assert(options.credentials.accountName, "Missing accountName");
  assert(options.credentials.accountKey ||
         options.credentials.sas,         "Missing accountKey or sas");

  // Identify the parent class, that is always `this` so we can use it on
  // subclasses
  var Parent = this;

  // Create a subclass of Parent
  var subClass = function(entity) {
    // Always pass down the entity we're initializing from
    Parent.call(this, entity);
  };
  util.inherits(subClass, Parent);

  // Inherit class methods too (ie. static members)
  _.assign(subClass, Parent);

  // Validate that subclass is already configured
  assert(
    subClass.prototype.__version      &&
    subClass.prototype.__mapping      &&
    subClass.prototype.__deserialize  &&
    subClass.prototype.__partitionKey &&
    subClass.prototype.__rowKey,
    "Must be configured first, see `Entity.configure`"
  );

  // Don't allow setup to run twice, there is no reasons for this. In particular
  // it could give issues access properties
  assert(
    !subClass.prototype.__tableName &&
    !subClass.prototype.__client,
    "This `Entity` subclass is already setup!"
  );

  // Add accountUrl, if not already present, there is really no reason to
  // not just compute... That's what the Microsoft libraries does anyways
  var credentials = _.defaults({}, options.credentials, {
    accountUrl: [
      "https://",
      options.credentials.accountName,
      ".table.core.windows.net/"
    ].join('')
  });
  assert(/^https:\/\//.test(credentials.accountUrl), "Use HTTPS for accountUrl");
  var client = azureTable.createClient(credentials);

  // Set tableName and client
  subClass.prototype.__client     = client;
  subClass.prototype.__tableName  = options.tableName;

  // Make some auxiliary methods bound to tableName and denodified
  var aux = {};
  [
    'createTable',
    'deleteTable',
    'deleteEntity',
    'insertEntity',
    'updateEntity',
    'mergeEntity',
    'getEntity',

  ].forEach(function(method) {
    aux[method] = Promise.denodeify(client[method].bind(
      client,
      options.tableName
    ));
  });

  // Set auxiliary methods
  subClass.prototype.__aux = aux;

  // Define access properties, we do this here, as doing it in Entity.configure
  // means that it could be called more than once. When subclassing with new
  // versions, we don't really want that.
  _.forIn(subClass.prototype.__mapping, function(type, property) {
    // Define property for accessing underlying shadow object
    Object.defineProperty(subClass.prototype, property, {
      enumerable: true,
      get:        function() {return this.__properties[property];}
    });
  });

  // Return subClass
  return subClass;
};

/** Create the underlying Azure Storage Table, errors if it exists */
Entity.createTable = function() {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");

  return ClassProps.__aux.createTable({
    ignoreIfExists:     false
  }).catch(rethrowDebug(
    "createTable: Failed to create table '%s' with err: %j",
    ClassProps.__tableName
  ));
};

/** Ensure existence of the underlying Azure Storage Table */
Entity.ensureTable = function() {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");

  return ClassProps.__aux.createTable({
    ignoreIfExists:     true
  }).catch(rethrowDebug(
    "ensureTable: Failed to create table '%s' with err: %j",
    ClassProps.__tableName
  ));
};

/** Delete the underlying Azure Storage Table */
Entity.removeTable = function(ignoreErrors) {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");

  return ClassProps.__aux.deleteTable({
    ignoreIfExists:     true
  }).catch(rethrowDebug(
    "deleteTable: Failed to delete table '%s' with err: %j",
    ClassProps.__tableName
  ));
};

/**
 * Create an entity on azure table with property and mapping.
 * Returns a promise for an instance of `this` (typically an Entity subclass)
 */
Entity.create = function(properties) {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");
  assert(properties,              "Properties is required");

  // Construct entity with built-in properties
  var entity = {
    PartitionKey:     ClassProps.__partitionKey.exact(properties),
    RowKey:           ClassProps.__rowKey.exact(properties),
    Version:          ClassProps.__version
  };

  // Add custom properties to entity
  _.forIn(ClassProps.__mapping, function(type, property) {
    type.serialize(entity, properties[property]);
  });

  return ClassProps.__aux.insertEntity(entity)
  .catch(rethrowDebug("Failed to insert entity: %j err: %j", entity))
  .then(function(etag) {
    entity.__etag = etag;     // Add etag
    return entity;
  })
  .then(wrapEntityClass(Class));
};

/**
 * Load Entity subclass from azure given PartitionKey and RowKey,
 * This method return a promise for the subclass instance.
 */
Entity.load = function(properties) {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");
  assert(properties,              "Properties is required");

  // Serialize partitionKey and rowKey
  var partitionKey  = ClassProps.__partitionKey.exact(properties);
  var rowKey        = ClassProps.__rowKey.exact(properties);

  return ClassProps.__aux.getEntity(
    partitionKey,
    rowKey
  ).then(wrapEntityClass(Class));
};


/**
 * Remove entity without loading it. Using this method you cannot quantify about
 * the remote state you're deleting. Using `Entity.prototype.remove` removal
 * will fail, if the remove entity has been modified.
 */
Entity.remove = function(properties) {
  var Class       = this;
  var ClassProps  = Class.prototype;
  assert(ClassProps.__client,     "Entity not setup, see Entity.setup()");
  assert(properties,              "Properties is required");

  return ClassProps.__aux.deleteEntity({
    PartitionKey:     ClassProps.__partitionKey.exact(properties),
    RowKey:           ClassProps.__rowKey.exact(properties),
    __etag:           undefined
  }).catch(rethrowDebug("Failed to delete entity, err: %j"));
};


/** Remove entity if not modified, unless `ignoreChanges` is set */
Entity.prototype.remove = function(ignoreChanges) {
  return this.__aux.deleteEntity({
    PartitionKey:     this.__partitionKey.exact(this.__properties),
    RowKey:           this.__rowKey.exact(this.__properties),
    __etag:           ignoreChanges ? undefined : this.__etag
  }, {
    force:            ignoreChanges ? true : false
  }).catch(rethrowDebug("Failed to delete entity, err: %j"));
};

/**
 * Update the entity by fetching its values again, returns true of there was
 * any changes.
 */
Entity.prototype.update = function() {
  var that = this;
  var etag = this.__etag;

  // Serialize partitionKey and rowKey
  var partitionKey  = this.__partitionKey.exact(this.__properties);
  var rowKey        = this.__rowKey.exact(this.__properties);

  return this.__aux.getEntity(
    partitionKey,
    rowKey
  ).then(function(entity) {
    // Deserialize a shadow object from the entity
    that.__properties = that.__deserialize(entity);

    // Set __etag
    that.__etag = entity.__etag;

    // Check if any properties was modified
    return that.__etag !== etag;
  });
};

/**
 * Modify an entity, the `modifier` is a function that is called with
 * a clone of the entity as `this`, it should apply modifications to `this`.
 * This function shouldn't have side-effects (or these should be contained),
 * as the `modifier` may be called more than once, if the update operation
 * fails.
 *
 * This method will apply modified to a clone of the current data and attempt to
 * save it. But if this fails because the entity have been updated by another
 * process (ie. etag is out of date), it'll reload the entity from azure table.
 * invoke the modifier again and try to save again. This model fit very well
 * with the optimistic concurrency model used in Azure Table Storage.
 *
 * **Note** modifier may return a promise.
 */
Entity.prototype.modify = function(modifier) {
  var that = this;

  // Serialize partitionKey and rowKey
  var partitionKey  = this.__partitionKey.exact(this.__properties);
  var rowKey        = this.__rowKey.exact(this.__properties);

  // Create a clone of this.__properties, that can be used to compare properties
  // and restore state, if operations fail
  var properties    = {};
  var etag          = this.__etag;
  _.forIn(this.__mapping, function(type, property) {
    properties[property] = type.clone(this.__properties[property]);
  }, this);

  // Attempt to modify this object
  var attemptsLeft = MAX_MODIFY_ATTEMPTS;
  var attemptModify = function() {
    var modified = Promise.resolve(modifier.call(that.__properties));
    return modified.then(function() {
      var isChanged     = false;
      var entityChanges = {};

      // Check if `that.__properties` have been changed and serialize changes to
      // `entityChanges` while flagging changes in `isChanged`
      _.forIn(that.__mapping, function(type, property) {
        var value = that.__properties[property];
        if (type.equal(properties[property], value)) {
          type.serialize(entityChanges, value);
          isChanged = true;
        }
      });

      // Set etag
      entityChanges.__etag = that.__etag;

      // Check for changes
      if (!changed) {
        debug("Return modify trivially, as changed was applied by modifier");
        return that;
      }

      // Check for key modifications
      assert(partitionKey === that.__partitionKey.exact(that.__properties),
             "You can't modify elements of the partitionKey");
      assert(rowkey === that.__rowKey.exact(that.__properties),
             "You can't  modify elements of the rowKey");

      return that.__aux.mergeEntity(entityChanges).then(function(etag) {
        that.__etag = etag;
        return that;
      });
    }).catch(function(err) {
      // Restore internal state
      that.__etag = etag;
      that.__properties = properties;

      // rethrow error, if it's not caused by optimistic concurrency
      if (!err || err.code !== 'UpdateConditionNotSatisfied') {
        debug("Update of entity failed unexpected, err: %j", err, err.stack);
        throw err;
      }

      // Decrement number of attempts left
      attemptsLeft -= 1;
      if (attemptsLeft === 0) {
        debug("ERROR: MAX_MODIFY_ATTEMPTS exhausted, we might have congestion");
        throw new Error("MAX_MODIFY_ATTEMPTS exhausted, check for congestion");
      }

      // Reload and try again
      return that.__aux.getEntity(partitionKey, rowKey).then(function(entity) {
        // Deserialize properties and set etag
        that.__properties = that.__deserialize(entity);
        that.__etag = entity.__etag;

        // Attempt to modify again
        return attemptModify();
      });
    });
  };

  return attemptModify();
};




//TODO: Entity.query
//TODO: Entity.scan
































// TODO: Method upgrade all entities to new version in a background-process
//       This is useful for when something relies on filtering properties
//       and we change a property name.


// Export Entity
module.exports = Entity;



/*


Entity.load:
   - Requires exact partition- and row-keys (returns one only!)
   - no filtering employed

Entity.scan(properties, options)
   - Requires no partition key (and asserts that it's not provided)
   - Requires either exact, partial or no rowkey information
   - Filters on additional properties (not covered by rowkey)
{
  matchRow:            'exact' || 'partial' || 'none'.
  // Call handler with entities instead of returning them, maxEntities is
  // maxNumber to call in parallel (you can abort by )
  handler:             function(item) { return new Promise(...); }, // optional
  // Max number of entities returned, unless `handler` is provided
  maxEntities:         1000 // default
}

Entity.query(properties, options)
   - Requires exact partition key
   - Requires either exact, partial or no rowkey information
   - Filters on additional properties (not covered by keys)
  Note, if row key is exact, this is almost equivalent of Entity.load, except
  that Entity.load will not filter on additional properties, hence, this can
  be used an a conditional load.
{
  matchRow:             'exact', 'partial' || 'none',

  // Call handler with entities instead of returning them, maxEntities is
  // maxNumber to call in parallel (you can abort by )
  handler:             function(item) { return new Promise(...); }, // optional
  // Max number of entities returned, unless `handler` is provided
  maxEntities:         1000 // default
}



*/
