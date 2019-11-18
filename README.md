Azure Table Storage Entities
============================
[![Build Status](https://travis-ci.org/taskcluster/azure-entities.svg?branch=master)](https://travis-ci.org/taskcluster/azure-entities)

An elegant library for working with azure table storage as a cheap, consistent
key/value store.

Interface
---------

Azure exposes its storage in the form of tables.  This library wraps each table
in an entity class.  Instances of this class represent a row from the table.
The library defines several class methods and several instance methods.

Entity classes are created in two steps.  The configuration step defines the
shape of the table, and the setup step provides runtime information
(credentials, etc.) needed to access the data.

### Configure

The configure call returns the class, and takes options:
```js
{
  version:           2,                    // Version of the schema
  partitionKey:      Entity.HashKey('prop1'), // Partition key, can be StringKey
  rowKey:            Entity.StringKey('prop2', 'prop3'), // RowKey...
  properties: {
    prop1:           Entity.types.Blob,    // Properties and types
    prop2:           Entity.types.String,
    prop3:           Entity.types.Number,
    prop4:           Entity.types.JSON,
    prop5:           Entity.types.Boolean,
    prop6:           Entity.types.Schema({ // Same as JSON, but enforces schema validation
      type: 'object',
      properties: {
        myKey: {type: 'string'},
      }
      additionalProperties: false,
      required: ['myKey'],
    }),
  },
  signEntities:      false,                // HMAC sign entities
  context: [                               // Required context keys
    'prop7'                                // Constant specified in setup()
  ],
  migrate: function(itemV1) {              // Migration function, if not v1
    return // transform item from version 1 to version 2
  },
}
```

This might be used like this:

```js
var Entity = require('azure-entities');

// Create an abstract key-value pair
var AbstractKeyValue = Entity.configure({
  version:     1,
  partitionKey:    Entity.StringKey('key'),
  rowKey:          Entity.ConstantKey('kv-pair'),
  properties: {
    key:           Entity.types.String,
    value:         Entity.types.JSON
  }
});

// Overwrite the previous definition AbstractKeyValue with a new version
AbstractKeyValue = AbstractKeyValue.configure({
  version:         2,
  partitionKey:    Entity.StringKey('key'),
  rowKey:          Entity.ConstantKey('kv-pair'),
  properties: {
    key:           Entity.types.String,
    date:          Entity.types.Date
  },
  migrate: function(item) {
    // Translate from version 1 to version 2
    return {
      key:      item.key,
      date:     new Date(item.value.StringDate)
    };
  }
});
```

`AbstractKeyValue` is the resulting entity class.

#### Property Types

The example above shows a few entity types.  The full list, all properties of
`Entity.types`, is:

  * `String`
  * `Number`
  * `Date`
  * `UUID`
  * `SlugId`
  * `Boolean`
  * `Blob` -- binary blob
  * `Text` -- arbitrary text
  * `JSON` -- JSONable data
  * `Schema(s)` -- JSON matching the JSON schema `s`
  * `SlugIdArray` -- an array of slugids

The following types are encrypted, and require additional arguments to the
`setup` method, below.

  * `EncryptedText`
  * `EncryptedBlob`
  * `EncryptedJSON`
  * `EncryptedSchema(s)` -- JSON matching the JSON schema `s`

Note that all entity types have a maximum stored size of 256k.  Do not store
values of unbounded size in a single row.

Note that the arbitrary-sized property types, such as String and Blob, can result in an error with `err.code === 'PropertyTooLarge'` if the property is too large.

#### Keys

The `partitionKey` and `rowKey` options are used to describe how the Azure
partition and row keys are generated from the properties.  The Azure
documentation contains more information on the semantics of partition and row
keys.  The available types are:

  * `StringKey(prop)` -- use a single string property as the key
  * `ConstantKey(const)` -- use a constant value as the key
  * `CompositeKey(...props)` -- use a sequence of properties to create the key
  * `HashKey(...props)` -- use a hash of a sequence of properties to create the key

StringKey is the simplest option, and indicates that one property should be treated as the key.

ConstantKey is useful to "ignore" a key field for tables that do not have enough columns to represent both a partition and row key.
It is typically used as a rowKey, with a StringKey as the partitionKey, effectively storing each row in a unique partition and allowing Azure to distribute partitions across servers as needed.

CompositeKey and HashKey are similar, and combine multiple properties into a single key.
CompositeKey uses string concatenation and thus could conceivably support prefix matching, althoug this is not implemented.
HashKey hashes the input properties to a fixed length and is useful for large or unbounded properties.

#### Migrations

The library supports in-place schema migrations.  When doing this, you must
base it on the previous version, and you must increment version number by 1 and
only 1.

After a migration, it's your responsibility that `partitionKey` and
`rowKey` will keep returning the same value, otherwise you cannot migrate
entities on-the-fly, but must take your application off-line while you upgrade
the data schema.  Or start submitting data to an additional table, while you're
migrating existing data in an off-line process.

#### Context

Notice that it is possible to require custom context properties to be injected
with `Entity.setup` using the `context` option. This option takes a list of
property names. These property names must then be specified with
`Entity.setup({context: {myProp: ...}})`. This is a good way to inject
configuration keys and constants for use in Entity instance methods.

### Setup

The `setup` method creates a new subclass of `this` (`Entity` or subclass
thereof) that is ready for use, with the following options:


```js
{
  credentials:       ...                 // see below
  tableName:         "AzureTableName",   // Azure table name
  agent:             https.Agent,        // Agent to use (default a global)
  signingKey:        "...",              // Key for HMAC signing entities
  cryptoKey:         "...",              // Key for encrypted properties
  drain:             base.stats.Influx,  // Statistics drain (optional)
  component:         '<name>',           // Component in stats (if drain)
  process:           'server',           // Process in stats (if drain)
  context:           {...}               // Extend prototype (optional)
  monitor:           new Monitor(..),    // Monitor instance (optional)
  operationReportChance: 0.0,            // Chance that an arbitrary transaction will be logged
  operationReportThreshold: 10 * 1000,   // Time in milliseconds over which a transaction will be logged
}
```

In `Entity.configure` the `context` options is a list of property names,
these properties **must** be specified in when `Entity.setup` is called.
They will be used to extend the subclass prototype. This is typically used to
inject configuration constants for use in Entity instance methods.

Once you have configured properties, version, migration, keys, using
`Entity.configure`, you can call `Entity.setup` on your new subclass.  This
will again create a new subclass that is ready for use, with azure credentials,
etc. This new subclass cannot be configured further, nor can `setup` be
called again.

#### Credentials

Credentials can be specified to this library in a variety of ways.  Note that
these match those of the
[fast-azure-storage](https://github.com/taskcluster/fast-azure-storage)
library, except for `inMemory`.

##### Raw Azure credentials

Given an accountName and accompanying account key, configure access like this:

```js
{
  // Azure connection details
  tableName: "AzureTableName",
  // Azure credentials
  credentials: {
    accountId: "...",
    accessKey: "...",
  },
}
```

##### SAS Function

The underlying
[fast-azure-storage](https://github.com/taskcluster/fast-azure-storage) library
allows use of SAS credentials, including dynamic generation of SAS credentials
as needed. That support can be used transparently from this library:

```js
{
  tableName: 'AzureTableName',
  credentials: {
    accountId: '...',
    sas: sas   // sas in querystring form: "se=...&sp=...&sig=..."
  };
}
```

or

```js
{
  tableName: 'AzureTableName',
  credentials: {
    accountId: '...',
    sas: function() {
      return new Promise(/* fetch SAS from somewhere */);
    },
    minSASAuthExpiry:   15 * 60 * 1000 // time before refreshing the SAS
  };
}
```

##### Testing

To use an in-memory, testing-oriented table, use the special credential
`inMemory`. 

```js
{
  tableName:       "AzureTableName"
  credentials: "inMemory",
}
```

This testing implementation is largely true to Azure, but is intended only for
testing, and only in combination with integration tests against Azure to reveal
any unknown inconsistencies.

#### Monitoring

The `setup` function takes an optional `monitor` argument, which if given
should be an object with methods `monitor.measure(name, value)` and
`monitor.count(name)` to measure a named value, and to count a named event,
respectively.  These methods will be used to measure the duration and number of
calls to each Azure API method (`getEntity`, etc.), using names of the form
`<apiMethod>.<result>` where result is `error` or `success`.

The `operationReportChance` and `operationReportThreshold` options control the
frequency of debug logging about API method timing, and may be removed from the
library soon.

### Table Operations

To ensure that the underlying Azure table actually exists, call
`ensureTable`.  This is an idempotent operation, and is often called in
service start-up. If you've used taskcluster-auth to get credentials
rather than azure credentials, do not use this as taskcluster-auth has
already ensured the table exists for you.

```js
await MyEntity.ensureTable()
```

To remove a table, call `removeTable`.  Note that Azure does not allow
re-creation of a table until some time after the remove operation returns.

### Row Operations

The `create` method creates a new row.  Its first argument gives the
properties for the new row.  If its second argument is true, it will overwrite
any existing row with the same primary key.

```js
await MyEntity.create({
    prop1: "val1",
    prop2: "val2",
}, true);
```

The `modify` method modifies a row, given a modifier.  The modifier is a
function that is called with a clone of the entity as `this` and first
argument, it should apply modifications to `this` (or first argument).  This
function shouldn't have side-effects (or these should be contained), as the
`modifier` may be called more than once, if the update operation fails.

This method will apply `modified` to a clone of the current data and attempt
to save it. But if this fails because the entity have been updated by another
process (the ETag is out of date), it'll reload the entity from the Azure
table, invoke the modifier again, and try to save again. This model fits very
well with the optimistic concurrency model used in Azure Table Storage.

**Note** modifier is allowed to return a promise.

```js
await entity.modify(function() {
  this.property = "new value";
});
```

Or using first argument, when binding modifier or using ES6 arrow-functions:

```js
await entity.modify(function(entity) {
  entity.property = "new value";
});
```

Note that the arbitrary-sized property types, such as String and Blob, can result in an error with `err.code === 'PropertyTooLarge'` on creation or modification if the property is too large.

The `remove` method will remove a row.  This can be called either as a class
method (in which case the row is not loaded) or as an instance method.  Both
methods have `ignoreIfNotExists` as a second argument, and if true this will
cause the method to return successfully if the row is not present.

```js
await MyEntity.remove({id: myThingId})
```

The instance method takes `row.remove(ignoreChanges, ignoreIfNotExists)`,
where `ignoreChanges` will ignore the case where the row has been updated
since it was loaded.

```js
row = await MyEntity.load({id: myThingId})
// ...
row.remove()
```

### Queries

The `load` method will turn a single existing entity, given enough properties
to determine the row key and partition key.  The method will throw an error if
the row does not exist, unless its second argument is true.

```js
var entity = await MyEntity.load({id: myThingId});
var maybe = await MyEntity.load({id: myThingId}, true);
```

An existing row has a `reload` method which will load the properties from the
table once more, and return true if anything has changed.

```js
var updated = entity.reload();
```

The `scan` method will scan the entire table, filtering on properties and
possibly accelerated with partitionKey and rowKey indexes.

You can use this in two ways: with a handler or without a handler.  In the
latter case you'll get a list of up to 1000 entries and a continuation token to
restart the scan from.

To scan **without a handler** call `Entity.scan(conditions, options)` as
illustrated below:

```js
data = await Entity.scan({
  prop1:              Entity.op.equal('val1'),  // Filter on prop1 === 'val1'
  prop2:              "val2",                   // Same as Entity.op.equal
  prop3:              Entity.op.lessThan(42)    // Filter on prop3 < 42
}, {
  matchPartition:     'none',       // Require 'exact' or 'none' partitionKey
  matchRow:           'none',       // Require 'exact' or 'none' rowKey
  limit:              1000,         // Max number of entries
  continuation:       undefined     // Continuation token to scan from
});

data.entries        // List of Entities
data.continuation   // Continuation token, if defined
```

To scan **with a handler** call `Entity.scan(conditions, options)` as
follows:

```js
await MyEntity.scan({
  prop1:              Entity.op.equal('val1'),  // Filter on prop1 === 'val1'
  prop2:              "val2",                   // Same as Entity.op.equal
  prop3:              Entity.op.lessThan(42)    // Filter on prop3 < 42
}, {
  continuation:       '...',        // Continuation token to continue from
  matchPartition:     'none',       // Require 'exact' or 'none' partitionKey
  matchRow:           'none',       // Require 'exact' or 'none' rowKey
  limit:              1000,         // Max number of parallel handler calls
  handler:            function(item) {
    return new Promise(...); // Do something with the item
  }
});
```

The available operations for conditions, all properties of `Entity.op`, are:

 * `equal`
 * `notEqual`
 * `lessThan`
 * `lessThanOrEqual`
 * `greaterThan`
 * `greaterThanOrEqual`

**Configuring match levels**, the options `matchPartition` and `matchRow` can
be used specify match levels. If left as `'none'` (default), the scan will not
use Partition- or Row-Key indexes for acceleration.

If you specify `matchRow: 'exact'`, conditions must contain enough equality
constraints to build the expected row-key, which will then be used to
accelerate the table scan.

If the conditions doesn't specify enough equality constraints to build the
exact row-key, and error will be thrown. This allows you to reason about
expected performance.

**Continuation token**, if using `Entity.scan` without a handler, you receive a
continuation token in the `continuation` property of the return value. You can
use this to continue the table scan. A continuation token is a a string that
matches `Entity.continuationTokenPattern`.  You can use this pattern to detect
invalid continuation tokens from your users and offer a suitable error message.

The `query` method is exactly the same as `Entity.scan` except
`matchPartition` is set to to `'exact'`. This means that conditions
**must** provide enough constraints for constructions of the partition-key.

This is provided as a special function, because `Entity.scan` shouldn't be
used for on-the-fly queries, when `matchPartition: 'none'`. As
`Entity.scan` will do a full table scan, which is only suitable in background
workers.

If you use `Entity.query` you don't run the risk of executing a full table
scan. But depending on the size of your partitions it may still be a lengthy
operation. Always query with care.

# Development

To work on the `azure-entities` library itself, you will need an Azure account.
Azure provides a ["free tier"](https://azure.microsoft.com/en-us/free/), or you
may contact the Taskcluster developers to get a testing credential for the
Taskcluster account.

If you are setting up your own account, you will need to create a storage
account and create an access key for it.

Set the environment variables `AZURE_ACCOUNT_KEY` and `AZURE_ACCOUNT_ID`
appropriately before running the tests.

To get started developing, install [yarn](http://yarnpkg.com/) and the newest
major version of Node, and run `yarn` in the root of the repository to install
dependencies.  Then run `yarn test` to start the tests.
