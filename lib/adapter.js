import oracledb from 'oracledb'
import utils from './utils'
import async from 'async'
import Sequel from 'waterline-sequel'
import _ from 'lodash'
let createcount = 0

oracledb.outFormat = oracledb.OBJECT;

const adapter = {

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
  registerConnection(connection, collections, cb) {
    if (!connection.identity) return cb(new Error('no identity'));
    if (this.connections.has(connection.identity)) return cb(new Error('too many connections'));

    let cxn = {
      identity: connection.identity,
      collections: collections || {},
      schema: adapter.buildSchema(connection, collections)
    }

    // set up some default values based on defaults for node-oracledb client
    const poolMin = connection.poolMin >= 0 ? connection.poolMin : 1
    const poolMax = connection.poolMax > 0 ? connection.poolMax : 4
    const poolIncrement = connection.poolIncrement > 0 ? connection.poolIncrement : 1
    const poolTimeout = connection.poolTimeout >= 0 ? connection.poolTimeout : 1
    const stmtCacheSize = connection.stmtCacheSize >=0 ? connection.stmtCacheSize : 30
    const prefetchRows = connection.prefetchRows >=0 ? connection.prefetchRows : 100
    const enableStats = connection.enableStats ? true : false

    if (connection.maxRows > 0) {
      oracledb.maxRows = connection.maxRows
    }
    if (connection.queueTimeout >= 0) {
      oracledb.queueTimeout = connection.queueTimeout
    }


    const poolconfig = {
      _enableStats: enableStats,
      user: connection.user,
      password: connection.password,
      connectString: connection.connectString,
      poolMin,
      poolMax,
      poolIncrement,
      poolTimeout,
      stmtCacheSize
    }

    // set up connection pool
    oracledb.createPool(poolconfig, (err, pool) => {
      if (err) return cb(err)
      cxn.pool = pool
      this.connections.set(cxn.identity, cxn)
      cb()
    })
  },

  /**
   * Construct the waterline schema for the given connection.
   *
   * @param connection
   * @param collections[]
   */
  buildSchema (connection, collections) {
    return _.chain(collections)
      .map((model, modelName) => {
        let definition = _.get(model, [ 'waterline', 'schema', model.identity ])
        return _.defaults(definition, {
          attributes: { },
          tableName: modelName
        })
      })
      .keyBy('tableName')
      .value()
  },

  /**
   * Create a new table
   *
   * @param connectionName
   * @param collectionName
   * @param definition - the waterline schema definition for this model
   * @param cb
   */
  define(connectionName, collectionName, definition, cb) {
    let cxn = this.connections.get(connectionName)
    let collection = cxn.collections[collectionName]
    if (!collection) return cb(new Error(`No collection with name ${collectionName}`))

    let queries = []
    let query = {
      sql: `CREATE TABLE "${collectionName}" (${utils.buildSchema(definition)})`
    }
    queries.push(query)

    // Handle auto-increment
    const autoIncrementFields = utils.getAutoIncrementFields(definition)

    if (autoIncrementFields.length > 0) {
      // Create sequence and trigger queries for each one
      autoIncrementFields.forEach(field => {
        let sequenceName = utils.getSequenceName(collectionName, field)
        queries.push({
          sql: `CREATE SEQUENCE ${sequenceName}`
        })
        let triggerSql = `CREATE OR REPLACE TRIGGER ${collectionName}_${field}_trg
                         BEFORE INSERT ON "${collectionName}"
                         FOR EACH ROW
                         BEGIN
                         SELECT ${sequenceName}.NEXTVAL
                         INTO :new."${field}" FROM dual;
                         END;`

        queries.push({
          sql: triggerSql
        })
      })
    }

    // need to create sequence and trigger for auto increment
    return this.executeQuery(connectionName, queries, cb)

  },

  describe(connectionName, collectionName, cb) {
    let connectionObject = this.connections.get(connectionName)
    let collection = connectionObject.collections[collectionName]

    // have to search for triggers/sequences?
    let queries = [];
    queries.push({
      sql: `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '${collectionName}'`
    })
    queries.push({
      sql: `SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE TABLE_NAME = '${collectionName}'`
    })
    queries.push({
      sql: `SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name =
        '${collectionName}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
        ORDER BY cols.table_name, cols.position`,
    })

    this.executeQuery(connectionName, queries, (err, results) => {
      const schema = results[0]
      const indices = results[1]
      const tablePrimaryKeys = results[2]
      let normalized = utils.normalizeSchema(schema, collection.definition)
      if (_.isEmpty(normalized)) {
        return cb()
      }
      cb(null, normalized)
    })
  },

  executeQuery(connectionName, queries, cb) {

    if (!_.isArray(queries)) {
      queries = [queries]
    }
    let cxn = this.connections.get(connectionName)
    if (cxn.pool._enableStats) {
      console.log(cxn.pool._logStats())
    }
    cxn.pool.getConnection((err, conn) => {

      if (err && err.message.indexOf('ORA-24418') > -1) {
        // In this scenario, just keep trying until one of the connections frees up
        return setTimeout(this.executeQuery(connectionName, queries, cb).bind(this), 50)
      }

      if (err) return cb(err)

      async.reduce(queries, [], (memo, query, asyncCallback) => {
          let options = {}

          // Autocommit by default
          if (query.autoCommit !== false) {
            options.autoCommit = true
          }

          if (query.outFormat !== undefined) {
            options.outFormat = query.outFormat
          }

          //console.log('executing', query.sql, query.params)
          conn.execute(query.sql, query.params || [], options, (queryError, res) => {
            if (queryError) return asyncCallback(queryError)
            memo.push(res)
            asyncCallback(null, memo)
          })
        }, (asyncErr, result) => {
            conn.release((error) => {
              if (error) console.log('Problem releasing connection', error)
              if (asyncErr) {
                cb(asyncErr)
              } else {
                cb(null, this.handleResults(result))
              }
            })
        })
    })
  },

  handleResults(results) {
    return results.length == 1 ? results[0] : results
  },

  teardown(conn, cb) {
    if (typeof conn == 'function') {
      cb = conn
      conn = null
    }
    if (conn === null) {
      this.connections = {}
      return cb()
    }
    if (!this.connections.has(conn)) return cb()

    const cxn = this.connections.get(conn)
    cxn.pool.close().then(() => {
      this.connections.delete(conn)
      cb()
    })
    .catch(cb)
  },

  createEach(connectionName, table, records, cb) {
    cb()
  },

  // Add a new row to the table
  create(connectionName, table, data, cb) {
    let connectionObject = this.connections.get(connectionName)
    let collection = connectionObject.collections[table];

    let schemaName = collection.meta && collection.meta.schemaName ? utils.escapeName(collection.meta.schemaName) + '.' : '';
    let tableName = schemaName + utils.escapeName(table);

    // Build up a SQL Query
    let schema = connectionObject.schema;
    //var processor = new Processor(schema);

    // Prepare values
    Object.keys(data).forEach(function(value) {
      data[value] = utils.prepareValue(data[value]);
    });

    const definition = collection.definition
    _.each(definition, (column, name) => {

      if (fieldIsBoolean(column)) {
        // no boolean type in oracle, so save it as a number
        data[name] = (data[name]) ? 1 : 0
      }

    })

    let sequel = new Sequel(schema, this.sqlOptions)

    let incrementSequences = [];
    let query;

    try {
      query = sequel.create(table, data);
    } catch (e) {
      return cb(e);
    }

    let returningData = utils.getReturningData(collection.definition)

    let queryObj = {}

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ')
      query.values = query.values.concat(returningData.params)
      queryObj.outFormat = oracledb.OBJECT
    }

    queryObj.sql = query.query
    queryObj.params = query.values

    this.executeQuery(connectionName, queryObj, (err, results) => {
        if (err) return cb(err)
        cb(null, utils.transformBulkOutbinds(results.outBinds, returningData.fields)[0])
      })
  },

  find(connectionName, collectionName, options, cb, connection) {
    const connectionObject = this.connections.get(connectionName)
    const collection = connectionObject.collections[collectionName]

    let sequel = new Sequel(connectionObject.schema, this.sqlOptions)
    let query
    let limit = options.limit || null;
    let skip = options.skip || null;
    delete options.skip;
    delete options.limit;


    // Build a query for the specific query strategy
    try {
      query = sequel.find(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    let findQuery = query.query[0]

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
      }, (err, results) => {
        if (err) return cb(err)
        cb(null, results && results.rows)
      })

  },

  destroy(connectionName, collectionName, options, cb, connection) {
    const connectionObject = this.connections.get(connectionName)
    const collection = connectionObject.collections[collectionName]

    let query;
    const sequel = new Sequel(connectionObject.schema, this.sqlOptions);

    try {
      query = sequel.destroy(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    const handler = (err, findResult) => {
      if (err) return cb(err)
      this.executeQuery(connectionName, {
          sql: query.query,
          params: query.values
        }, (delErr, delResult) => {
          // TODO: verify delResult?
          if (delErr) return cb(delErr)
          cb(null, findResult)
        })
    }

    return this.find(connectionName, collectionName, options, handler, connection);

  },

  drop(connectionName, collectionName, relations, cb, connection) {

    if (typeof relations == 'function') {
      cb = relations
      relations = []
    }

    relations.push(collectionName)

    let queries = relations.reduce((memo, tableName) => {
      memo.push({
        sql: `DROP TABLE "${tableName}"`
      })
      const connectionObject = this.connections.get(connectionName)
      const collection = connectionObject.collections[tableName]

      const autoIncrementFields = utils.getAutoIncrementFields(collection.definition)
      if (autoIncrementFields.length > 0) {
        autoIncrementFields.forEach((field) => {
          let sequenceName = utils.getSequenceName(tableName, field)
          memo.push({
            sql: `DROP SEQUENCE ${sequenceName}`
          })
        })
      }
      return memo
    }, [])

    return this.executeQuery(connectionName, queries, cb)
  },

  update(connectionName, collectionName, options, values, cb, connection) {
    //    var processor = new Processor();
    const connectionObject = this.connections.get(connectionName);
    const collection = connectionObject.collections[collectionName];

    // Build find query
    const sequel = new Sequel(connectionObject.schema, this.sqlOptions);

    let query
    // Build query
    try {
      query = sequel.update(collectionName, options, values);
    } catch (e) {
      return cb(e);
    }

    let returningData = utils.getReturningData(collection.definition)
    let queryObj = {}

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ')
      query.values = query.values.concat(returningData.params)
    }

    queryObj.sql = query.query
    queryObj.params = query.values


    // Run query
    return this.executeQuery(connectionName, queryObj, (err, results) => {
        if (err) return cb(err)
        cb(null, utils.transformBulkOutbinds(results.outBinds, returningData.fields))
      })

  }
}

function fieldIsBoolean(column) {
  return (!_.isUndefined(column.type) && column.type === 'boolean');
}

export default adapter
