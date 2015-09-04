'use strict';

Object.defineProperty(exports, '__esModule', {
  value: true
});

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { 'default': obj }; }

var _oracledb = require('oracledb');

var _oracledb2 = _interopRequireDefault(_oracledb);

var _utils = require('./utils');

var _utils2 = _interopRequireDefault(_utils);

var _bluebird = require('bluebird');

var _bluebird2 = _interopRequireDefault(_bluebird);

var _waterlineSequel = require('waterline-sequel');

var _waterlineSequel2 = _interopRequireDefault(_waterlineSequel);

var _lodash = require('lodash');

var _lodash2 = _interopRequireDefault(_lodash);

var adapter = {

  connections: new Map(),

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

    var cxn = {
      identity: connection.identity,
      collections: collections || {}
    };

    // set up pool here
    _oracledb2['default'].createPool({
      user: connection.user,
      password: connection.password,
      connectString: connection.connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 10,
      stmtCacheSize: 10
    }, function (err, pool) {
      if (err) return cb(err);
      cxn.pool = pool;
      _this.connections.set(cxn.identity, cxn);
      cb();
    });
  },

  define: function define(connectionName, collectionName, definition, cb) {
    var cxn = this.connections.get(connectionName);
    var collection = cxn.collections[collectionName];
    if (!collection) return cb(new Error('No collection with name ' + collectionName));

    var queries = [];
    var query = {
      sql: 'CREATE TABLE ' + collectionName + ' (' + _utils2['default'].buildSchema(definition) + ')'
    };
    queries.push(query);

    // are there any fields that require auto increment?
    var autoIncrementFields = _utils2['default'].getAutoIncrementFields(definition);
    if (autoIncrementFields.length > 0) {
      // create sequence and trigger queries for each one
      autoIncrementFields.forEach(function (field) {
        var sequenceName = collectionName + '_seq';
        //queries.push({sql: `ALTER TABLE ${collectionName} ADD (CONSTRAINT ${collectionName}_pk PRIMARY KEY ("${field}"))`})
        queries.push({
          sql: 'CREATE SEQUENCE ' + sequenceName
        });
        var triggerSql = 'CREATE OR REPLACE TRIGGER ' + collectionName + '_trg\n                         BEFORE INSERT ON ' + collectionName + '\n                         FOR EACH ROW\n                         BEGIN\n                         SELECT ' + sequenceName + '.NEXTVAL\n                         INTO :new."' + field + '" FROM dual; \n                         END;';
        queries.push({
          sql: triggerSql,
          params: []
        });
      });
    }

    // need to create sequence and trigger for auto increment
    this.executeQuery(connectionName, queries).nodeify(cb);
  },

  describe: function describe(connectionName, collectionName, cb) {
    collectionName = collectionName.toUpperCase();
    // have to search for triggers/sequences?
    var queries = [];
    queries[0] = {
      sql: 'SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = \'' + collectionName + '\'',
      params: []
    };
    queries[1] = {
      sql: "SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE table_name = :name",
      params: [collectionName]
    };
    queries[2] = {
      sql: 'SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner\n        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = \n        :name AND cons.constraint_type = \'P\' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner\n        ORDER BY cols.table_name, cols.position',
      params: [collectionName]
    };

    return this.executeQuery(connectionName, queries).spread(function (schema, indices, tablePrimaryKeys) {
      cb(null, _utils2['default'].normalizeSchema(schema));
    })['catch'](cb);
  },

  executeQuery: function executeQuery(connectionName, queries) {
    var _this2 = this;

    return new _bluebird2['default'](function (resolve, reject) {
      if (!_lodash2['default'].isArray(queries)) {
        queries = [queries];
      }
      var cxn = _this2.connections.get(connectionName);
      cxn.pool.getConnection(function (err, conn) {
        if (err) return reject(err);
        return _bluebird2['default'].reduce(queries, function (memo, query) {
          return new _bluebird2['default'](function (resolve, reject) {
            var options = {};

            // autocommit by default
            if (query.autoCommit !== false) {
              options.autoCommit = true;
            }

            if (query.outFormat !== undefined) {
              options.outFormat = query.outFormat;
            }
            conn.execute(query.sql, query.params || [], options, function (queryError, res) {
              if (queryError) return reject(queryError);
              memo.push(res);
              resolve(memo);
            });
          });
        }, []).then(function (result) {
          conn.release(function (err) {
            if (err) return reject(err);
            resolve(_this2.handleResults(result));
          });
        })['catch'](function (err) {
          conn.release(function (error) {
            if (error) console.log('problem releasing connection', error);
            reject(err);
          });
        });
      });
    });
  },

  handleResults: function handleResults(results) {
    return results.length == 1 ? results[0] : results;
  },

  teardown: function teardown(conn, cb) {
    if (typeof conn == 'function') {
      cb = conn;
      conn = null;
    }
    if (conn === null) {
      this.connections = {};
      return cb();
    }
    if (!this.connections[conn]) return cb();
    delete this.connections[conn];
    cb();
  },

  // Add a new row to the table
  create: function create(connectionName, table, data, cb) {

    var connectionObject = this.connections.get(connectionName);
    var collection = connectionObject.collections[table];

    var schemaName = collection.meta && collection.meta.schemaName ? _utils2['default'].escapeName(collection.meta.schemaName) + '.' : '';
    var tableName = schemaName + _utils2['default'].escapeName(table).toUpperCase();

    // Build up a SQL Query
    var schema = connectionObject.schema;
    //var processor = new Processor(schema);

    /*
          // Mixin WL Next connection overrides to sqlOptions
          var overrides = connectionOverrides[connectionName] || {};
          var options = _.cloneDeep(sqlOptions);
          if (hop(overrides, 'wlNext')) {
            options.wlNext = overrides.wlNext;
          }
    */

    var sequel = new _waterlineSequel2['default'](schema, {
      escapeInserts: true
    } /*options*/);

    var incrementSequences = [];
    var query;

    // Build a query for the specific query strategy
    try {
      query = sequel.create(table.toUpperCase(), data);
    } catch (e) {
      return cb(e);
    }

    // is there an autoincrementing key?  if so, we have to add a returning parameter to grab it
    var returningData = _lodash2['default'].reduce(collection.definition, function (memo, attributes, field) {
      if (attributes.autoIncrement === true) {
        memo.params.push({
          type: _oracledb2['default'][_utils2['default'].sqlTypeCast(attributes.type)],
          dir: _oracledb2['default'].BIND_OUT
        });
        memo.fields.push('"' + field + '"');
        memo.outfields.push(':' + field);
        memo.rawFields.push(field);
      }
      return memo;
    }, {
      params: [],
      fields: [],
      rawFields: [],
      outfields: []
    });

    var queryObj = {};

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ');
      query.values = query.values.concat(returningData.params);
      queryObj.outFormat = _oracledb2['default'].OBJECT;
    }

    queryObj.sql = _utils2['default'].alterParameterSymbol(query.query);
    queryObj.params = query.values;

    this.executeQuery(connectionName, queryObj).then(function (result) {
      /*
       * results! { rowsAffected: 1,
       *   outBinds: [ [ 2 ] ],
       *     rows: undefined,
       *       metaData: undefined }
       */
      // join the returning fields with the out params
      returningData.rawFields.forEach(function (field, index) {
        data[field] = result.outBinds[index][0];
      });
      cb(null, data);
    })['catch'](cb);
    /*
    // Run Query
    client.query(query.query, query.values, function __CREATE__(err, result) {
      if (err) return cb(handleQueryError(err));
       // Cast special values
      var values = processor.cast(table, result.rows[0]);
       // Set Sequence value to defined value if needed
      if (incrementSequences.length === 0) return cb(null, values);
       function setSequence(item, next) {
        var sequenceName = "'\"" + table + '_' + item + '_seq' + "\"'";
        var sequenceValue = values[item];
        var sequenceQuery = 'SELECT setval(' + sequenceName + ', ' + sequenceValue + ', true)';
         client.query(sequenceQuery, function(err, result) {
          if (err) return next(err);
          next();
        });
      }
       async.each(incrementSequences, setSequence, function(err) {
        if (err) return cb(err);
        cb(null, values);
      });
    }, cb);
    */
  }

};

exports['default'] = adapter;
module.exports = exports['default'];