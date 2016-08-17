'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

var _oracledb = require('oracledb');

var _oracledb2 = _interopRequireDefault(_oracledb);

var _utils = require('./utils');

var _utils2 = _interopRequireDefault(_utils);

var _async = require('async');

var _async2 = _interopRequireDefault(_async);

var _waterlineSequel = require('waterline-sequel');

var _waterlineSequel2 = _interopRequireDefault(_waterlineSequel);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var createcount = 0;

_oracledb2['default'].outFormat = _oracledb2['default'].OBJECT;

var adapter = {

  connections: new Map(),

  syncable: true,
  schema: true,

  sqlOptions: {
    parameterized: true,
    caseSensitive: false,
    escapeCharacter: '"',
    casting: false,
    canReturnValues: false,
    escapeInserts: true,
    declareDeleteAlias: false,
    explicitTableAs: false,
    prefixAlias: 'alias__',
    stringDelimiter: "'",
    rownum: true,
    paramCharacter: ':',
    convertDate: false
  },

  /**
   * This method runs when a connection is initially registered
   * at server-start-time. This is the only required method.
   *
   * @param  {[type]}   connection [description]
   * @param  {[type]}   collection [description]
   * @param  {Function} cb         [description]
   * @return {[type]}              [description]
   */
  registerConnection: function registerConnection(connection, collections, cb) {
    var _this = this;

    if (!connection.identity) return cb(new Error('no identity'));
    if (this.connections.has(connection.identity)) return cb(new Error('too many connections'));

    var cxn = {
      identity: connection.identity,
      collections: collections || {},
      schema: adapter.buildSchema(connection, collections)
    };

    // set up some default values based on defaults for node-oracledb client
    var poolMin = connection.poolMin >= 0 ? connection.poolMin : 1;
    var poolMax = connection.poolMax > 0 ? connection.poolMax : 4;
    var poolIncrement = connection.poolIncrement > 0 ? connection.poolIncrement : 1;
    var poolTimeout = connection.poolTimeout >= 0 ? connection.poolTimeout : 1;
    var stmtCacheSize = connection.stmtCacheSize >= 0 ? connection.stmtCacheSize : 30;
    var prefetchRows = connection.prefetchRows >= 0 ? connection.prefetchRows : 100;
    var enableStats = connection.enableStats ? true : false;

    if (connection.maxRows > 0) {
      _oracledb2['default'].maxRows = connection.maxRows;
    }
    if (connection.queueTimeout >= 0) {
      _oracledb2['default'].queueTimeout = connection.queueTimeout;
    }

    var poolconfig = {
      _enableStats: enableStats,
      user: connection.user,
      password: connection.password,
      connectString: connection.connectString,
      poolMin: poolMin,
      poolMax: poolMax,
      poolIncrement: poolIncrement,
      poolTimeout: poolTimeout,
      stmtCacheSize: stmtCacheSize
    };

    // set up connection pool
    _oracledb2['default'].createPool(poolconfig, function (err, pool) {
      if (err) return cb(err);
      cxn.pool = pool;
      _this.connections.set(cxn.identity, cxn);
      cb();
    });
  },

  /**
   * Construct the waterline schema for the given connection.
   *
   * @param connection
   * @param collections[]
   */
  buildSchema: function buildSchema(connection, collections) {
    return _lodash2['default'].chain(collections).map(function (model, modelName) {
      var definition = _lodash2['default'].get(model, ['waterline', 'schema', model.identity]);
      return _lodash2['default'].defaults(definition, {
        attributes: {},
        tableName: modelName
      });
    }).keyBy('tableName').value();
  },

  /**
   * Create a new table
   *
   * @param connectionName
   * @param collectionName
   * @param definition - the waterline schema definition for this model
   * @param cb
   */
  define: function define(connectionName, collectionName, definition, cb) {
    var cxn = this.connections.get(connectionName);
    var collection = cxn.collections[collectionName];
    if (!collection) return cb(new Error('No collection with name ' + collectionName));

    var queries = [];
    var query = {
      sql: 'CREATE TABLE "' + collectionName + '" (' + _utils2['default'].buildSchema(definition) + ')'
    };
    queries.push(query);

    // Handle auto-increment
    var autoIncrementFields = _utils2['default'].getAutoIncrementFields(definition);

    if (autoIncrementFields.length > 0) {
      // Create sequence and trigger queries for each one
      autoIncrementFields.forEach(function (field) {
        var sequenceName = _utils2['default'].getSequenceName(collectionName, field);
        queries.push({
          sql: 'CREATE SEQUENCE ' + sequenceName
        });
        var triggerSql = 'CREATE OR REPLACE TRIGGER ' + collectionName + '_' + field + '_trg\n                         BEFORE INSERT ON "' + collectionName + '"\n                         FOR EACH ROW\n                         BEGIN\n                         SELECT ' + sequenceName + '.NEXTVAL\n                         INTO :new."' + field + '" FROM dual;\n                         END;';

        queries.push({
          sql: triggerSql
        });
      });
    }

    // need to create sequence and trigger for auto increment
    return this.executeQuery(connectionName, queries, cb);
  },

  describe: function describe(connectionName, collectionName, cb) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    // have to search for triggers/sequences?
    var queries = [];
    queries.push({
      sql: 'SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = \'' + collectionName + '\''
    });
    queries.push({
      sql: 'SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE TABLE_NAME = \'' + collectionName + '\''
    });
    queries.push({
      sql: 'SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\n        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name =\n        \'' + collectionName + '\' AND cons.constraint_type = \'P\' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner\n        ORDER BY cols.table_name, cols.position'
    });

    this.executeQuery(connectionName, queries, function (err, results) {
      var schema = results[0];
      var indices = results[1];
      var tablePrimaryKeys = results[2];
      var normalized = _utils2['default'].normalizeSchema(schema, collection.definition);
      if (_lodash2['default'].isEmpty(normalized)) {
        return cb();
      }
      cb(null, normalized);
    });
  },

  executeQuery: function executeQuery(connectionName, queries, cb) {
    var _this2 = this;

    if (!_lodash2['default'].isArray(queries)) {
      queries = [queries];
    }
    var cxn = this.connections.get(connectionName);
    if (cxn.pool._enableStats) {
      console.log(cxn.pool._logStats());
    }
    cxn.pool.getConnection(function (err, conn) {

      if (err && err.message.indexOf('ORA-24418') > -1) {
        // In this scenario, just keep trying until one of the connections frees up
        return setTimeout(_this2.executeQuery(connectionName, queries, cb).bind(_this2), 50);
      }

      if (err) return cb(err);

      _async2['default'].reduce(queries, [], function (memo, query, asyncCallback) {
        var options = {};

        // Autocommit by default
        if (query.autoCommit !== false) {
          options.autoCommit = true;
        }

        if (query.outFormat !== undefined) {
          options.outFormat = query.outFormat;
        }

        //console.log('executing', query.sql, query.params)
        conn.execute(query.sql, query.params || [], options, function (queryError, res) {
          if (queryError) return asyncCallback(queryError);
          memo.push(res);
          asyncCallback(null, memo);
        });
      }, function (asyncErr, result) {
        conn.release(function (error) {
          if (error) console.log('Problem releasing connection', error);
          if (asyncErr) {
            cb(asyncErr);
          } else {
            cb(null, _this2.handleResults(result));
          }
        });
      });
    });
  },

  handleResults: function handleResults(results) {
    return results.length == 1 ? results[0] : results;
  },

  teardown: function teardown(conn, cb) {
    var _this3 = this;

    if (typeof conn == 'function') {
      cb = conn;
      conn = null;
    }
    if (conn === null) {
      this.connections = {};
      return cb();
    }
    if (!this.connections.has(conn)) return cb();

    var cxn = this.connections.get(conn);
    cxn.pool.close().then(function () {
      _this3.connections['delete'](conn);
      cb();
    })['catch'](cb);
  },

  createEach: function createEach(connectionName, table, records, cb) {
    cb();
  },

  // Add a new row to the table
  create: function create(connectionName, table, data, cb) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var schemaName = collection.meta && collection.meta.schemaName ? _utils2['default'].escapeName(collection.meta.schemaName) + '.' : '';
    var tableName = schemaName + _utils2['default'].escapeName(table);

    // Build up a SQL Query
    var schema = connectionObject.schema;
    //var processor = new Processor(schema);

    // Prepare values
    Object.keys(data).forEach(function (value) {
      data[value] = _utils2['default'].prepareValue(data[value]);
    });

    var definition = collection.definition;
    _lodash2['default'].each(definition, function (column, name) {

      if (fieldIsBoolean(column)) {
        // no boolean type in oracle, so save it as a number
        data[name] = data[name] ? 1 : 0;
      }
    });

    var sequel = new _waterlineSequel2['default'](schema, this.sqlOptions);

    var incrementSequences = [];
    var query = undefined;

    try {
      query = sequel.create(table, data);
    } catch (e) {
      return cb(e);
    }

    var returningData = _utils2['default'].getReturningData(collection.definition);

    var queryObj = {};

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ');
      query.values = query.values.concat(returningData.params);
      queryObj.outFormat = _oracledb2['default'].OBJECT;
    }

    queryObj.sql = query.query;
    queryObj.params = query.values;

    this.executeQuery(connectionName, queryObj, function (err, results) {
      if (err) return cb(err);
      cb(null, _utils2['default'].transformBulkOutbinds(results.outBinds, returningData.fields)[0]);
    });
  },

  find: function find(connectionName, collectionName, options, cb, connection) {
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);
    var query = undefined;
    var limit = options.limit || null;
    var skip = options.skip || null;
    delete options.skip;
    delete options.limit;

    // Build a query for the specific query strategy
    try {
      query = sequel.find(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    var findQuery = query.query[0];

    if (limit && skip) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER > ' + skip + ' and LINE_NUMBER <= ' + (skip + limit);
    } else if (limit) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER <= ' + limit;
    } else if (skip) {
      findQuery = 'SELECT * FROM (' + findQuery + ') WHERE LINE_NUMBER > ' + skip;
    }

    this.executeQuery(connectionName, {
      sql: findQuery,
      params: query.values[0]
    }, function (err, results) {
      if (err) return cb(err);
      cb(null, results && results.rows);
    });
  },

  destroy: function destroy(connectionName, collectionName, options, cb, connection) {
    var _this4 = this;

    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    var query = undefined;
    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);

    try {
      query = sequel.destroy(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    var handler = function handler(err, findResult) {
      if (err) return cb(err);
      _this4.executeQuery(connectionName, {
        sql: query.query,
        params: query.values
      }, function (delErr, delResult) {
        // TODO: verify delResult?
        if (delErr) return cb(delErr);
        cb(null, findResult);
      });
    };

    return this.find(connectionName, collectionName, options, handler, connection);
  },

  drop: function drop(connectionName, collectionName, relations, cb, connection) {
    var _this5 = this;

    if (typeof relations == 'function') {
      cb = relations;
      relations = [];
    }

    relations.push(collectionName);

    var queries = relations.reduce(function (memo, tableName) {
      memo.push({
        sql: 'DROP TABLE "' + tableName + '"'
      });
      var connectionObject = _this5.connections.get(connectionName);
      var collection = connectionObject.collections[tableName];

      var autoIncrementFields = _utils2['default'].getAutoIncrementFields(collection.definition);
      if (autoIncrementFields.length > 0) {
        autoIncrementFields.forEach(function (field) {
          var sequenceName = _utils2['default'].getSequenceName(tableName, field);
          memo.push({
            sql: 'DROP SEQUENCE ' + sequenceName
          });
        });
      }
      return memo;
    }, []);

    return this.executeQuery(connectionName, queries, cb);
  },

  update: function update(connectionName, collectionName, options, values, cb, connection) {
    //    var processor = new Processor();
    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[collectionName];

    // Build find query
    var sequel = new _waterlineSequel2['default'](connectionObject.schema, this.sqlOptions);

    var query = undefined;
    // Build query
    try {
      query = sequel.update(collectionName, options, values);
    } catch (e) {
      return cb(e);
    }

    var returningData = _utils2['default'].getReturningData(collection.definition);
    var queryObj = {};

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ');
      query.values = query.values.concat(returningData.params);
    }

    queryObj.sql = query.query;
    queryObj.params = query.values;

    // Run query
    return this.executeQuery(connectionName, queryObj, function (err, results) {
      if (err) return cb(err);
      cb(null, _utils2['default'].transformBulkOutbinds(results.outBinds, returningData.fields));
    });
  }
};

function fieldIsBoolean(column) {
  return !_lodash2['default'].isUndefined(column.type) && column.type === 'boolean';
}

exports['default'] = adapter;
module.exports = exports['default'];