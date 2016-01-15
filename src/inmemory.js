/**
 * In-memory version of the `__aux` wrapper used in entity.js.  This
 * is basically implementing the bits of fast-azure-entities that we
 * use in azure-entities.
 */

var _         = require('lodash');
var crypto    = require('crypto');
var stringify = require('json-stable-stringify');

// the in-memory data; stored globally as this is a better model for
// Azure than storing each table as an instance or class property
var tables = {};

var InMemoryWrapper = function InMemoryWrapper(table) {
  this.table = table;
};

/* Internal utilities */

var odataPrefix = /^odata\./;
var odataSuffix = /@odata\.type$/;
var entityEtag = function(entity) {
  // always filter out odata.* metadata
  entity = _.omit(entity, function(v, k) { return odataPrefix.test(k); });

  // include an entity type for each attribute
  _.forIn(entity, function(v, k) {
    if (odataSuffix.test(k)) {
      return;
    }
    var odata_type = k + '@odata.type';
    if (entity[odata_type]) {
      return;
    }
    if (typeof v === "boolean") {
      entity[odata_type] = 'Edm.Boolean';
    } else if (typeof v === "number") {
      if (/\./.test(v)) {
        entity[odata_type] = "Edm.Double";
      } else {
        entity[odata_type] = "Edm.Int32";
      }
    } else {
      entity[odata_type] = "Edm.String";
    }
  });

  // hash the resulting object to make the etag
  var sha1 = crypto.createHash('sha1');
  sha1.update(stringify(entity));
  return sha1.digest('hex');
};

var makeError = function(statusCode, code) {
  var err = new Error(code);
  err.statusCode = statusCode;
  err.code = code;
  return err;
};

var makeKey = function(partitionKey, rowKey) {
  return partitionKey.replace(/!/g, '!!') + '!' + rowKey.replace(/!/g, '!!');
};

/**
 * Create table.
 *
 * @method createTable
 * @return {Promise} A promise that the table was created.
 */
InMemoryWrapper.prototype.createTable = function() {
  tables[this.table] = {};
  // TODO: EntityAlready Exists?
  return Promise.resolve();
};

/**
 * Delete table.
 *
 * @method deleteTable
 * @return {Promise} A promise that the table was marked for deletion.
 */
InMemoryWrapper.prototype.deleteTable = function() {
  if (!tables[this.table]) {
    return Promise.reject(makeError(404, 'ResourceNotFound'));
  }
  delete tables[this.table];
  return Promise.resolve();
};

/**
 * Get entity with given `partitionKey` and `rowKey`.
 *
 * @method getEntity
 * @param {string} partitionKey - Partition key of entity to get.
 * @param {string} rowKey - Row key of entity to get.
 * @param {object} options - Options on the following form:
 * ```js
 * {
 *   select:  ['key1', ...],  // List of keys to return (defaults to all)
 *   filter:  '...'           // Filter string for conditional load
 * }
 * ```
 * @return {Promise}
 * A promise for the entity, form of the object depends on the meta-data
 * level configured and if `select` as employed. See Azure documentation for
 * details.
 */
InMemoryWrapper.prototype.getEntity = function(partitionKey, rowKey, options) {
  // TODO: options
  var key = makeKey(partitionKey, rowKey);
  if (!tables[this.table]) {
    return Promise.reject(makeError(404, 'ResourceNotFound'));
  }
  if (key in tables[this.table]) {
    var res = _.clone(tables[this.table][makeKey(partitionKey, rowKey)]);
    res['odata.etag'] = entityEtag(res);
    return Promise.resolve(res);
  }
  return Promise.reject(makeError(404, 'ResourceNotFound'));
};

/**
 * Query entities.
 *
 * @method queryEntitites
 * @param {object} options - Options on the following form:
 * ```js
 * {
 *   // Query options:
 *   select:            ['key1', ...],  // Keys to $select (defaults to all)
 *   filter:            'key1 eq true', // $filter string, see Table.filter
 *   top:               1000,           // Max number of entities to return
 *
 *   // Paging options:
 *   nextPartitionKey:  '...',          // nextPartitionKey from previous result
 *   nextRowKey:        '...'           // nextRowKey from previous result
 * }
 * ```
 * @return {Promise} A promise for an object on the form:
 * ```js
 * {
 *   entities: [
 *     {
 *       // Keys selected from entity and meta-data depending on meta-data level
 *     },
 *     ...
 *   ],
 *   nextPartitionKey: '...',  // Opaque token for paging
 *   nextRowKey:       '...'   // Opaque token for paging
 * }
 * ```
 */
InMemoryWrapper.prototype.queryEntities = function() {
  // TODO
};

/**
 * Insert `entity`. The `entity` object must be on the format
 * accepted by azure table storage. See Azure Table Storage documentation for
 * details. Essentially, data-types will be inferred if `...@odata.type`
 * properties aren't specified. Also note that `PartitionKey` and `RowKey`
 * properties must be specified.
 *
 * @method insertEntity
 * @param {object} entity - Entity object, see Azure Table Storage
 * documentation for details on how to annotate types.
 * @return {Promise}
 * A promise for the `etag` of the inserted entity.
 */
InMemoryWrapper.prototype.insertEntity = function(entity) {
  var key = makeKey(entity.PartitionKey, entity.RowKey);
  if (!tables[this.table]) {
    return Promise.reject(makeError(404, 'ResourceNotFound'));
  }
  if (key in tables[this.table]) {
    return Promise.reject(makeError(409, 'EntityAlreadyExists'));
  }
  tables[this.table][key] = entity;
  return Promise.resolve(entityEtag(entity));
};

/**
 * Update entity identified by `entity.partitionKey` and
 * `entity.rowKey`.
 * Options are **required** for this method and takes form as follows:
 * ```js
 * {
 *   mode:  'replace' || 'merge'  // Replace entity or merge entity
 *   eTag:  '...' || '*' || null  // Update specific entity, any or allow insert
 * }
 * ```
 *
 * If `options.mode` is `'replace'` the remote entity will be completely
 * replaced by the structure given as `entity`. If `options.mode` is `'merge'`
 * properties from `entity` will overwrite existing properties on remote entity.
 *
 * If **`options.eTag` is not given** (or `null`) the remote entity will be
 * inserted if it does not exist, and otherwise replaced or merged depending
 * on `mode`.
 *
 * If **`options.eTag` is the string `'*'`** the remote entity will be replaced
 * or merged depending on `mode`, but it will not be inserted if it doesn't
 * exist.
 *
 * If **`options.eTag` is a string** (other than `'*'`) the remote entity will be
 * replaced or merged depending on `mode`, if the ETag of the remote entity
 * matches the string given in `options.eTag`.
 *
 * Combining `mode` and `eTag` options this method implements the following
 * operations:
 *  * Insert or replace (regardless of existence or ETag),
 *  * Replace if exists (regardless of ETag),
 *  * Replace if exists and has given ETag,
 *  * Insert or merge (regardless of existence or ETag),
 *  * Merge if exists (regardless of ETag), and
 *  * Merge if exists and has given ETag.
 *
 * @method updateEntity
 * @param {object} options - Options on the following form:
 * ```js
 * {
 *   mode:  'replace' || 'merge'  // Replace entity or merge entity
 *   eTag:  '...' || '*' || null  // Update specific entity, any or allow insert
 * }
 * ```
 * @return {Promise} A promise for `eTag` of the modified entity.
 */
InMemoryWrapper.prototype.updateEntity = function(entity, options) {
  var key = makeKey(entity.PartitionKey, entity.RowKey);
  if (!tables[this.table]) {
    return Promise.reject(makeError(404, 'ResourceNotFound'));
  }
  if (key in tables[this.table]) {
    if (options.eTag != '*') {
      if (options.eTag && options.eTag != entityEtag(tables[this.table][key])) {
        return Promise.reject(makeError(412, 'UpdateConditionNotSatisfied'));
      }
    }
    if (options.mode == 'replace') {
      tables[this.table][key] = entity;
    } else {
      var existing = tables[this.table][key];
      _.forIn(entity, function(val, prop) {
        existing[prop] = val;
      });
      entity = existing;
    }
  } else {
    if (!options.eTag) {
      tables[this.table][key] = entity;
    } else if (options.eTag != '*') {
      return Promise.reject(makeError(500, 'NotSureWhatToDo'));
    }
  }
  return Promise.resolve(entityEtag(entity));
};

InMemoryWrapper.prototype.deleteEntity = function() {
};

module.exports = InMemoryWrapper;
