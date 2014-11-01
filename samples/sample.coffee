MongoSpace = require '../lib/mongo_tuplespace'
debug = require('debug')('linda:mongo-tuplespace:sample')
mongoose = require 'mongoose'
mongoose.connect 'mongodb://localhost', ->
  space = MongoSpace.space 'hoge'
  space2 = MongoSpace.space 'hoge'
  space3 = MongoSpace.space 'fuga'
  spaces = MongoSpace.spaces

  space.write {hoge: 'fuga'}
  debug space

  debug spaces
