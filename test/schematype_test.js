var subject = require('../src/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var crypto  = require('crypto');
var helper  = require('./helper');

let schema = {
  type: 'object',
  properties: {
    text: {type: 'string'},
    number: {type: 'integer'},
  },
  additionalProperties: false,
  required: ['text', 'number'],
};

helper.contextualSuites('Entity', [
  {
    context: 'Schema',
    options: {
      type: subject.types.Schema(schema),
    },
  }, {
    context: 'EncryptedSchema',
    options: {
      type: subject.types.EncryptedSchema(schema),
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
    }).setup(_.defaults({}, config, options.config));

    setup(function() {
      return Item.ensureTable();
    });

    test('schema match', async () => {
      let id    = slugid.v4();
      let data  = {text: 'Hello World', number: 42};
      await Item.create({
        id,
        name: 'test',
        data,
      });
    });

    [
      {
        text: 'float not integer',
        data: {text: 'Hello World', number: 42.4},
      }, {
        text: 'integer not string',
        data: {text: 5, number: 42},
      }, {
        text: 'missing text property',
        data: {number: 42},
      }, {
        text: 'additional property',
        data: {text: 'Hello World', number: 42, wrong: false},
      }, {
        text: 'string not integer',
        data: {text: 'Hello World', number: '42'},
      },
    ].forEach(({text, data}) => test('schema mismatch (' + text + ')', async () => {
      try {
        let id    = slugid.v4();
        await Item.create({id, name: 'test', data});
      } catch (err) {
        assert(/schema validation failed/.test(err.toString()),
          'expected schema error');
        return;
      }
      assert(false, 'Expected an error');
    }));
  });
});
