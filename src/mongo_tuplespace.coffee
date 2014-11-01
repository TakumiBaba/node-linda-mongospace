mongoose = require 'mongoose'
Schema = mongoose.Schema
debug = require('debug')('linda:mongo-tuplespace')

TupleSchema = new Schema
  space: type: String
  data: {}
  createdAt: type: Date, default: Date.now
  isTaken: type: Boolean, default: false

TupleSchema.pre 'save', (next) ->
  debug @
  MongoSpace.space(@space).check @data
  next()

TupleSchema.static 'tupleMatch', (tuple) ->
  for k,v of tuple
    return false if @data[k] isnt v
  return true

Tuple = mongoose.model 'tuple', TupleSchema

class MongoSpace

  @spaces = {}
  @space: (name) ->
    return MongoSpace.spaces[name] or
           MongoSpace.spaces[name] = new MongoSpace name

  constructor: (@name='noname') ->
    @callbacks = []

  check: (tuple) ->
    for c, i in @callbacks
      for k, v in c.data
        if tuple.data[k] isnt v
          break
      if c.type is 'take'
        tuple.isTaken = true
      if c.type isnt 'watch'
        @callbacks.splice i, 1
      c.func null, tuple.data
    debug tuple

  write: (data, options={}) ->
    tuple = new Tuple
      space: @name
      data: data
    tuple.save (err) ->
      throw err if err

  take: (tuple, callback) ->
    Tuple.find {data: tuple}, (err, tuples) ->
      throw err if err
      if tuples.length > 0
        t = tuples[0]
        t.isTaken = true
        t.save (error) ->
          throw error if error
          callback null, t
      else
        @callbacks.push
          func: callback
          data: tuple
          type: 'take'

  read: (tuple, callback) ->
    Tuple.find {data: tuple}, (err, tuples) ->
      throw err if err


  watch: (tuple, callback) ->
    @callbacks.push
      func: callback
      data: tuple
      type: 'watch'

module.exports = MongoSpace
