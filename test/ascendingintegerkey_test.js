var subject = require('../lib/entity');
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var debug   = require('debug')('test:entity:ascendingintegerkey');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.AscendingIntegerKey('rev'),
  properties: {
    id:             subject.types.SlugId,
    rev:            subject.types.PositiveInteger,
    text:           subject.types.String,
  },
});

helper.contextualSuites('Entity (AscendingIntegerKey)',
  helper.makeContexts(Item), function(ctx, options) {
    let {Item} = options;
    let text = slugid.v4();

    setup(function() {
      return Item.ensureTable();
    });

    test('Item.create, Item.load', async () => {
      let id = slugid.v4();
      await Item.create({id, rev: 0, text});
      let item = await Item.load({id, rev: 0});
      assert(item.text === text);
    });

    test('Can\'t modify key', async () => {
      let id = slugid.v4();
      let item = await Item.create({id, rev: 0, text});
      try {
        await item.modify(item => {
          item.rev = 1;
        });
      } catch (err) {
        debug('expected error: %s', err);
        return;
      }
      assert(false, 'expected an error');
    });

    test('Can\'t use negative numbers', async () => {
      let id = slugid.v4();
      try {
        await Item.create({id, rev: -1, text});
      } catch (err) {
        debug('expected error: %s', err);
        return;
      }
      assert(false, 'expected an error');
    });

    test('Preserve ordering listing a partition', async () => {
      let id = slugid.v4();
      await Item.create({id, rev: 1, text: 'B'});
      await Item.create({id, rev: 14, text: 'D'});
      await Item.create({id, rev: 0, text: 'A'});
      await Item.create({id, rev: 2, text: 'C'});
      await Item.create({id, rev: 200, text: 'E'});
      let {entries} = await Item.query({id});
      let revs = entries.map(item => item.rev);
      assert(_.isEqual(revs, [0, 1, 2, 14, 200]), 'wrong revision order');
      assert(_.isEqual(entries.map(item => item.text), [
        'A', 'B', 'C', 'D', 'E',
      ]), 'wrong order of text properties');
    });
  });
