var subject = require("../lib/entity")
var assert  = require('assert');
var slugid  = require('slugid');
var _       = require('lodash');
var Promise = require('promise');
var crypto  = require('crypto');
var helper  = require('./helper');

var Item = subject.configure({
  version:          1,
  partitionKey:     subject.keys.StringKey('id'),
  rowKey:           subject.keys.StringKey('name'),
  properties: {
    id:             subject.types.String,
    name:           subject.types.String,
    data:           subject.types.SlugIdArray
  }
});

helper.contextualSuites("Entity (SlugIDArrayType)", helper.makeContexts(Item),
function(context, options) {
  var Item = options.Item;

  setup(function() {
    return Item.ensureTable();
  });

  test("SlugIdArray.push", function() {
    var arr = subject.types.SlugIdArray.create();
    var id = slugid.v4();
    arr.push(id);
  });

  test("SlugIdArray.toArray", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const slug1 = slugid.v4();
    const slug2 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);

    const arr = slugArray.toArray();

    assert(slug1 === arr[0], `Expected ${slug1}`);
    assert(slug2 === arr[1], `Expected ${slug2}`);
  });

  test("SlugIdArray.toArray (with 1k ids)", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const N = 1000;
    let slugids = [];

    for (let i = 0; i < N; i++) {
      const id = slugid.v4();

      slugArray.push(id);
      slugids.push(id)
    }

    const result = slugArray.toArray();

    for (let i = 0; i < N; i++) {
      assert(slugids[i] === result[i], `Expected ${slugids[i]}`);
    }
  });

  test("SlugIdArray.includes", function() {
    const slugArray = subject.types.SlugIdArray.create();

    const slug1 = slugid.v4();
    const slug2 = slugid.v4();
    const slug3 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);

    const result1 = slugArray.includes(slug1);
    const result2 = slugArray.includes(slug2);
    const result3 = slugArray.includes(slug3);

    assert(result1 === true, `Expected ${slug1}`);
    assert(result2 === true, `Expected ${slug2}`);
    assert(result3 === false, `Did not expect ${slug3}`);
  });

  test("SlugIdArray.shift", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const slug1 = slugid.v4();
    const slug2 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);

    const result = slugArray.shift();

    assert(slugArray.length === 1, 'Expected length 1');
    assert(slugArray.avail === 31, 'Expected avail 31');
    assert(result === slug1, `Expected ${slug1}`);
    assert(!slugArray.includes(slug1), `Did not expect ${slug1}`);
  });

  test("SlugIdArray.shift on an empty SlugIdArray", function() {
    const slugArray = subject.types.SlugIdArray.create();

    slugArray.shift();

    assert(slugArray.length === 0, 'Expected length 0');
    assert(slugArray.avail === 32, 'Expected avail 32');
  });

  test("SlugIdArray.pop", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const slug1 = slugid.v4();
    const slug2 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);

    const result = slugArray.pop();

    assert(slugArray.length === 1, 'Expected length 1');
    assert(slugArray.avail === 31, 'Expected avail 31');
    assert(slugArray.includes(slug1), `Expected ${slug1}`);
    assert(!slugArray.includes(slug2), `Did not expect ${slug2}`);
    assert(result === slug2, `Expected ${slug2}`);
  });

  test("SlugIdArray.slice", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const slug1 = slugid.v4();
    const slug2 = slugid.v4();
    const slug3 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);
    slugArray.push(slug3);

    const result = slugArray.slice(1, 3);

    assert(result.length === 2, 'Expected length 2');
    assert(result[0] === slug2, `Expected ${slug2}`);
    assert(result[1] === slug3, `Expected ${slug3}`);
  });

  test("SlugIdArray.slice with negative index", function() {
    const slugArray = subject.types.SlugIdArray.create();
    const slug1 = slugid.v4();
    const slug2 = slugid.v4();
    const slug3 = slugid.v4();

    slugArray.push(slug1);
    slugArray.push(slug2);
    slugArray.push(slug3);

    const result = slugArray.slice(-3, -1);

    assert(result.length === 2, 'Expected length 2');
    assert(result[0] === slug1, `Expected ${slug1}`);
    assert(result[1] === slug2, `Expected ${slug2}`);
  });

  test("SlugIdArray.pop on an empty SlugIdArray", function() {
    const slugArray = subject.types.SlugIdArray.create();

    slugArray.pop();

    assert(slugArray.length === 0, 'Expected length 0');
    assert(slugArray.avail === 32, 'Expected avail 32');
  });

  test("SlugIdArray.push (with 1k ids)", function() {
    var arr = subject.types.SlugIdArray.create();
    for(var i = 0; i < 1000; i++) {
      arr.push(slugid.v4());
    }
  });

  test("SlugIdArray.indexOf", function() {
    var arr = subject.types.SlugIdArray.create();
    var id = slugid.v4();
    arr.push(id);
    assert(arr.indexOf(id) !== -1);
  });

  test("SlugIdArray.indexOf (with 1k ids)", function() {
    var arr = subject.types.SlugIdArray.create();
    var list = [];
    for(var i = 0; i < 1000; i++) {
      var id = slugid.v4();
      list.push(id);
      arr.push(id);
    }
    list.forEach(function(id) {
      assert(arr.indexOf(id) !== -1, "Expected slugid to be present in array");
    });
    for(var i = 0; i < 1000; i++) {
      var id = slugid.v4();
      assert(arr.indexOf(id) === list.indexOf(id),
             "Slugid present but not pushed!!");
    }
  });

  test("SlugIdArray.remove", function() {
    var arr = subject.types.SlugIdArray.create();
    var list = [];
    for(var i = 0; i < 1000; i++) {
      var id = slugid.v4();
      list.push(id);
      arr.push(id);
    }
    list.forEach(function(id) {
      assert(arr.remove(id), "Expected slugid to be present");
    });
    list.forEach(function(id) {
      assert(arr.indexOf(id) === -1, "Expected slugid to be removed");
    });
  });

  test("SlugIdArray.clone", function() {
    var arr = subject.types.SlugIdArray.create();
    for(var i = 0; i < 200; i++) {
      arr.push(slugid.v4());
    }
    var arr2 = arr.clone();
    assert(arr.equals(arr2));

    var id = slugid.v4();
    arr.push(id);
    var id2 = slugid.v4();
    arr2.push(id2);

    assert(arr.indexOf(id) !== -1, "id in arr");
    assert(arr.indexOf(id2) === -1, "id2 not in arr");
    assert(arr2.indexOf(id) === -1, "id not in arr2");
    assert(arr2.indexOf(id2) !== -1, "id2 in arr2");
    assert(!arr.equals(arr2));
  });

  test("SlugIdArray.equals (with 1k ids)", function() {
    var arr = subject.types.SlugIdArray.create();
    var arr2 = subject.types.SlugIdArray.create();
    for(var i = 0; i < 1000; i++) {
      var id = slugid.v4();
      arr.push(id);
      arr2.push(id);
    }
    assert(arr.equals(arr2));
  });

  // Generate random slugIdArrays
  var randomSlugIdArray = function(length) {
    var arr = subject.types.SlugIdArray.create();
    for (var i = 0; i < length; i++) {
      arr.push(slugid.v4());
    }
    return arr;
  };

  test("small slugid array", function() {
    var id    = slugid.v4();
    var arr   = randomSlugIdArray(42);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   arr
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        assert(itemA.data.equals(itemB.data));
        assert(itemA.data.equals(arr));
      });
    });
  });


  test("large slugid array (4k ids, 64kb)", function() {
    var id    = slugid.v4();
    var arr   = randomSlugIdArray(4 * 1024);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   arr
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        assert(itemA.data.equals(itemB.data));
        assert(itemA.data.equals(arr));
      });
    });
  });

  test("large slugid array (8k ids, 128kb)", function() {
    var id    = slugid.v4();
    var arr   = randomSlugIdArray(8 * 1024);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   arr
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        assert(itemA.data.equals(itemB.data));
        assert(itemA.data.equals(arr));
      });
    });
  });

  test("large slugid array (16k ids, 256kb)", function() {
    var id    = slugid.v4();
    var arr   = randomSlugIdArray(16 * 1024);
    return Item.create({
      id:     id,
      name:   'my-test-item',
      data:   arr
    }).then(function(itemA) {
      return Item.load({
        id:     id,
        name:   'my-test-item'
      }).then(function(itemB) {
        assert(itemA.data.equals(itemB.data));
        assert(itemA.data.equals(arr));
      });
    });
  });
});
