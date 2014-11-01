var MongoSpace, Schema, Tuple, TupleSchema, debug, mongoose;

mongoose = require('mongoose');

Schema = mongoose.Schema;

debug = require('debug')('linda:mongo-tuplespace');

TupleSchema = new Schema({
  space: {
    type: String
  },
  data: {},
  createdAt: {
    type: Date,
    "default": Date.now
  },
  isTaken: {
    type: Boolean,
    "default": false
  }
});

TupleSchema.pre('save', function(next) {
  debug(this);
  MongoSpace.space(this.space).check(this.data);
  return next();
});

TupleSchema["static"]('tupleMatch', function(tuple) {
  var k, v;
  for (k in tuple) {
    v = tuple[k];
    if (this.data[k] !== v) {
      return false;
    }
  }
  return true;
});

Tuple = mongoose.model('tuple', TupleSchema);

MongoSpace = (function() {
  MongoSpace.spaces = {};

  MongoSpace.space = function(name) {
    return MongoSpace.spaces[name] || (MongoSpace.spaces[name] = new MongoSpace(name));
  };

  function MongoSpace(name) {
    this.name = name != null ? name : 'noname';
    this.callbacks = [];
  }

  MongoSpace.prototype.check = function(tuple) {
    var c, i, k, v, _i, _j, _len, _len1, _ref, _ref1;
    _ref = this.callbacks;
    for (i = _i = 0, _len = _ref.length; _i < _len; i = ++_i) {
      c = _ref[i];
      _ref1 = c.data;
      for (v = _j = 0, _len1 = _ref1.length; _j < _len1; v = ++_j) {
        k = _ref1[v];
        if (tuple.data[k] !== v) {
          break;
        }
      }
      if (c.type === 'take') {
        tuple.isTaken = true;
      }
      if (c.type !== 'watch') {
        this.callbacks.splice(i, 1);
      }
      c.func(null, tuple.data);
    }
    return debug(tuple);
  };

  MongoSpace.prototype.write = function(data, options) {
    var tuple;
    if (options == null) {
      options = {};
    }
    tuple = new Tuple({
      space: this.name,
      data: data
    });
    return tuple.save(function(err) {
      if (err) {
        throw err;
      }
    });
  };

  MongoSpace.prototype.take = function(tuple, callback) {
    return Tuple.find({
      data: tuple
    }, function(err, tuples) {
      var t;
      if (err) {
        throw err;
      }
      if (tuples.length > 0) {
        t = tuples[0];
        t.isTaken = true;
        return t.save(function(error) {
          if (error) {
            throw error;
          }
          return callback(null, t);
        });
      } else {
        return this.callbacks.push({
          func: callback,
          data: tuple,
          type: 'take'
        });
      }
    });
  };

  MongoSpace.prototype.read = function(tuple, callback) {
    return Tuple.find({
      data: tuple
    }, function(err, tuples) {
      if (err) {
        throw err;
      }
    });
  };

  MongoSpace.prototype.watch = function(tuple, callback) {
    return this.callbacks.push({
      func: callback,
      data: tuple,
      type: 'watch'
    });
  };

  return MongoSpace;

})();

module.exports = MongoSpace;
