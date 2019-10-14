var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var crypto  = require('crypto');
var helper  = require('./helper');

helper.contextualSuites('Entity', [
  {
    context: 'JSON',
    options: {
      type: subject.types.JSON,
    },
  }, {
    context: 'EncryptedJSON',
    options: {
      type: subject.types.EncryptedJSON,
      config: {
        cryptoKey: 'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w=',
      },
    },
  }, {
    context: 'Schema',
    options: {
      type: subject.types.Schema({type: 'object'}),
    },
  }, {
    context: 'EncryptedSchema',
    options: {
      type: subject.types.EncryptedSchema({type: 'object'}),
      config: {
        cryptoKey: 'Iiit3Y+b4m7z7YOmKA2iCbZDGyEmy6Xn42QapzTU67w=',
      },
    },
  },
], function(context, options) {
  helper.contextualSuites('', [
    {
      context: 'Azure',
      options: {
        credentials:  helper.cfg.azure,
        tableName:    helper.cfg.tableName,
      },
    }, {
      context: 'In-Memory',
      options: {
        tableName: 'items',
        credentials: 'inMemory',
      },
    },
  ], function(ctx, config) {
    let Item = subject.configure({
      version:          1,
      partitionKey:     subject.keys.StringKey('id'),
      rowKey:           subject.keys.StringKey('name'),
      properties: {
        id:             subject.types.String,
        name:           subject.types.String,
        data:           options.type,
      },
    });

    suiteSetup(function() {
      Item = Item .setup(_.defaults({}, config, options.config));
    });

    setup(function() {
      return Item.ensureTable();
    });

    // Construct a large string
    var randomString = function(kbytes) {
      var s = 'abcefsfcccsrcsdfsdfsfrfdefdwedwiedowijdwoeidnwoifneoifnweodnwoid';
      s += s; // 128
      s += s; // 256
      s += s; // 512
      s += s; // 1024
      var arr = [];
      for (var i = 0; i < kbytes; i++) {
        arr.push(s);
      }
      return arr.join('');
    };

    test('largeString helper', function() {
      var text  = randomString(64);
      assert(text.length === 1024 * 64);
    });

    test('small JSON object', function() {
      var id    = slugid.v4();
      var obj   = {text: 'Hello World', number: 42};
      return Item.create({
        id:     id,
        name:   'my-test-item',
        data:   obj,
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, obj));
        });
      });
    });

    test('large JSON object (62k)', function() {
      var id    = slugid.v4();
      var obj   = {text: randomString(62), number: 42};
      return Item.create({
        id:     id,
        name:   'my-test-item',
        data:   obj,
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, obj));
        });
      });
    });

    test('large JSON object (126k)', function() {
      var id    = slugid.v4();
      var obj   = {text: randomString(126), number: 42};
      return Item.create({
        id:     id,
        name:   'my-test-item',
        data:   obj,
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, obj));
        });
      });
    });

    test('large JSON object (255k)', function() {
      var id    = slugid.v4();
      var obj   = {text: randomString(255), number: 42};
      return Item.create({
        id:     id,
        name:   'my-test-item',
        data:   obj,
      }).then(function(itemA) {
        return Item.load({
          id:     id,
          name:   'my-test-item',
        }).then(function(itemB) {
          assert(_.isEqual(itemA.data, itemB.data));
          assert(_.isEqual(itemA.data, obj));
        });
      });
    });
  });
});
