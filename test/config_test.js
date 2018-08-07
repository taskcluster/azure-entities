var subject         = require('../src/entity');
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

  test('inMemory', function() {
    Item.setup({
      tableName: 'items',
      credentials: 'inMemory',
    });
  });
});
