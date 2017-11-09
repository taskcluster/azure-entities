var subject         = require('../lib/entity');
var assert          = require('assert');

suite('Config', function() {

  let Item = subject.configure({
    version:          1,
    partitionKey:     subject.keys.StringKey('id'),
    rowKey:           subject.keys.AscendingIntegerKey('rev'),
    properties: {
      id:             subject.types.SlugId,
      rev:            subject.types.PositiveInteger,
    },
  });

  test('inMemory with credentials', function() {
    Item.setup({
      account:   'inMemory',
      table:    'items',
      credentials: {clientId: 'foo', accountToken: 'bar'},
    });
  });

  test('inMemory with no credentials', function() {
    try {
      Item.setup({
        account:   'inMemory',
        table:    'items',
      });
      assert(false, 'Should have thrown an error!');
    } catch (e) {
      assert(e.name === 'AssertionError');
      assert(e.message === 'credentials should be specified even with inMemory, but can be null');
    }
  });

  test('inMemory with null credentials', function() {
    Item.setup({
      account:   'inMemory',
      table:    'items',
      credentials: null,
    });
  });

  test('inMemory with undefined credentials', function() {
    Item.setup({
      account:   'inMemory',
      table:    'items',
      credentials: undefined,
    });
  });
});
