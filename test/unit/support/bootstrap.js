/**
 * Support functions for helping with Postgres tests
 */

var oracle = require('oracledb'),
  _ = require('lodash'),
  BPromise = require('bluebird'),
  adapter = require('../../../dist/adapter');

var Support = module.exports = {};

/*
Support.SqlOptions = {
  parameterized: true,
  caseSensitive: true,
  escapeCharacter: '"',
  casting: true,
  canReturnValues: true,
  escapeInserts: true,
  declareDeleteAlias: false
};
*/

Support.Config = {
  connectString: process.env.DB_CONNECT_STRING,
  user: process.env.DB_USER,
  password: process.env.DB_PASS,
  maxRows: 500000,
  queueTimeout: 60000,
  poolMax: 50,
  poolMin: 2,
  enableStats: process.env.ENABLE_STATS
};

// Fixture Collection Def
Support.Collection = function(name, def) {
  var schemaDef = {};
  schemaDef[name] = Support.Schema(name, def);
  return {
    identity: name,
    tableName: name,
    connection: 'test',
    definition: def || Support.Definition,
    waterline: {
      schema: schemaDef
    }
  };
};

// Fixture Table Definition
Support.Definition = {
  field_1: {
    type: 'string'
  },
  field_2: {
    type: 'string'
  },
  id: {
    type: 'integer',
    autoIncrement: true,
    defaultsTo: 'AUTO_INCREMENT',
    primaryKey: true
  }
};

Support.Schema = function(name, def) {
  return {
    connection: 'test',
    identity: name,
    tableName: name,
    attributes: def || Support.Definition
  };
}

// Register and Define a Collection
Support.Setup = function(tableName, cb) {

  var collection = Support.Collection(tableName);

  var collections = {};
  collections[tableName] = collection;

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, function(err) {
    if (err) return cb(err);
    adapter.define('test', tableName, Support.Definition, function(err) {
      if (err) return cb(err);
      cb();
    });
  });
};

// Just register a connection
Support.registerConnection = function(tableNames, cb) {
  var collections = {};

  tableNames.forEach(function(name) {
    var collection = Support.Collection(name);
    collections[name] = collection;
  });

  var connection = _.cloneDeep(Support.Config);
  connection.identity = 'test';

  adapter.registerConnection(connection, collections, cb);
};

// Remove a table
Support.Teardown = function(tableName, cb) {
  oracle.getConnection(Support.Config, function(err, connection) {
    dropTable(tableName, connection, function(err) {
      if (err) return cb(err);

      adapter.teardown('test', function(err) {
        cb();
      });

    });
  });
};

// Return a client used for testing
Support.Client = function(cb) {
  oracle.getConnection(Support.Config, cb)
};

// Seed a record to use for testing
Support.Seed = function(tableName, cb) {
  oracle.getConnection(Support.Config, function(err, client) {
    createRecord(tableName, client, function(err) {

      client.release((err) => {
        if (err) console.log('error releasing connection', err)
      })

      if(err) {
        return cb(err);
      }

      cb();
    });
  });
};

Support.SeedMultipleRows = function(tableName, numRows, cb) {
  var createRecordPromise = BPromise.promisify(createRecord)
  var proms = []
  oracle.getConnection(Support.Config, function(err, client) {

    for (var i=0; i<numRows; i++) {
      proms.push(createRecordPromise(tableName, client))
    }

    return Promise.all(proms)
      .then(() => {
        cb()
      }).catch(cb)
  })

}


function dropTable(table, client, cb) {
  let escapedtable = '"' + table + '"';

  var query = "DROP TABLE " + escapedtable;
  client.execute(query, [], function() {
    dropSequence(table, client, cb)
  })
}

function dropSequence(table, client, cb) {
  var query = `DROP SEQUENCE ${table}_id_seq`
  client.execute(query, [], cb)
}


function createRecord(table, client, cb) {
  const query = `INSERT INTO "${table}" ("field_1", "field_2") values ('foo', 'bar')`;

  client.execute(query, [], {autoCommit: true}, cb);
}
