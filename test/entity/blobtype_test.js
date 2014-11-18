suite("Entity (BlobType)", function() {
  var assert  = require('assert');
  var slugid  = require('slugid');
  var _       = require('lodash');
  var Promise = require('promise');
  var base    = require('../../');

  var helper  = require('./helper');
  var cfg = helper.loadConfig();

  var Item = base.Entity.configure({
    version:          1,
    partitionKey:     base.Entity.keys.StringKey('id'),
    rowKey:           base.Entity.keys.StringKey('name'),
    properties: {
      id:             base.Entity.types.String,
      name:           base.Entity.types.String,
      data:           base.Entity.types.Blob
    }
  }).setup({
    credentials:  cfg.get('azure'),
    tableName:    cfg.get('azureTestTableName')
  });

  test("Item.create, item.modify, item.reload", function() {
    var id = slugid.v4();
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   new Buffer([0, 1, 2, 3])
    }).then(function(itemA) {
      console.log(itemA.data);
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        console.log(itemB.data);
      });
    });
  });
});
