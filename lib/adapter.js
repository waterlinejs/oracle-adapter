import oracledb from 'oracledb'
import utils from './utils'
import Promise from 'bluebird'
import Sequel from 'waterline-sequel'
import _ from 'lodash'

const adapter = {

  connections: new Map(),

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
    paramCharacter: ':'
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
    let cxn = {
      identity: connection.identity,
      collections: collections || {}
    }

    // set up pool here
    oracledb.createPool({
      user: connection.user,
      password: connection.password,
      connectString: connection.connectString,
      poolMin: 1,
      poolMax: 5,
      poolIncrement: 1,
      poolTimeout: 10,
      stmtCacheSize: 10
    }, (err, pool) => {
      if (err) return cb(err)
      cxn.pool = pool
      this.connections.set(cxn.identity, cxn)
      cb()
    })

  },

  define(connectionName, collectionName, definition, cb) {
    let cxn = this.connections.get(connectionName)
    let collection = cxn.collections[collectionName]
    if (!collection) return cb(new Error(`No collection with name ${collectionName}`))

    let queries = []
    let query = {
      sql: `CREATE TABLE "${collectionName}" (${utils.buildSchema(definition)})`
    }
    queries.push(query)

    // are there any fields that require auto increment?
    const autoIncrementFields = utils.getAutoIncrementFields(definition)
    if (autoIncrementFields.length > 0) {
      // create sequence and trigger queries for each one
      autoIncrementFields.forEach((field) => {
        let sequenceName = utils.getSequenceName(collectionName, field)
          //queries.push({sql: `ALTER TABLE ${collectionName} ADD (CONSTRAINT ${collectionName}_pk PRIMARY KEY ("${field}"))`})
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
          sql: triggerSql,
          params: []
        })

      })
    }

    // need to create sequence and trigger for auto increment
    this.executeQuery(connectionName, queries).nodeify(cb)

  },

  describe(connectionName, collectionName, cb) {
    let connectionObject = this.connections.get(connectionName)
    let collection = connectionObject.collections[collectionName]

    // have to search for triggers/sequences?
    let queries = [];
    queries.push({
      sql: `SELECT COLUMN_NAME, DATA_TYPE, NULLABLE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '${collectionName}'`,
      params: []
    })
    queries.push({
      sql: `SELECT index_name,COLUMN_NAME FROM user_ind_columns WHERE TABLE_NAME = '${collectionName}'`,
      params: []
    })
    queries.push({
      sql: `SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = 
        :name AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
        ORDER BY cols.table_name, cols.position`,
      params: [collectionName]
    })

    return this.executeQuery(connectionName, queries)
      .spread((schema, indices, tablePrimaryKeys) => {
        cb(null, utils.normalizeSchema(schema, collection.definition))
      })
      .catch(cb)
  },

  executeQuery(connectionName, queries) {
    oracledb.outFormat = oracledb.OBJECT;

    return new Promise((resolve, reject) => {
      if (!_.isArray(queries)) {
        queries = [queries]
      }
      let cxn = this.connections.get(connectionName)
      cxn.pool.getConnection((err, conn) => {
        if (err) return reject(err)
        return Promise.reduce(queries, (memo, query) => {
            return new Promise((resolve, reject) => {
              let options = {}

              // autocommit by default
              if (query.autoCommit !== false) {
                options.autoCommit = true
              }

              if (query.outFormat !== undefined) {
                options.outFormat = query.outFormat
              }
              // console.log('executing', query.sql, query.params)
              conn.execute(query.sql, query.params || [], options, (queryError, res) => {
                // console.log('query result', queryError, res)
                if (queryError) return reject(queryError)
                memo.push(res)
                resolve(memo)
              })
            })
          }, [])
          .then((result) => {
            conn.release((err) => {
              if (err) return reject(err)
              resolve(this.handleResults(result))
            })
          })
          .catch((err) => {
            conn.release((error) => {
              if (error) console.log('problem releasing connection', error)
              reject(err)

            })
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
    if (!this.connections[conn]) return cb()
    delete this.connections[conn]
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

    /*
          // Mixin WL Next connection overrides to sqlOptions
          var overrides = connectionOverrides[connectionName] || {};
          var options = _.cloneDeep(sqlOptions);
          if (hop(overrides, 'wlNext')) {
            options.wlNext = overrides.wlNext;
          }
    */

    let sequel = new Sequel(schema, this.sqlOptions)

    let incrementSequences = [];
    let query;

    // Build a query for the specific query strategy
    try {
      query = sequel.create(table, data);
    } catch (e) {
      return cb(e);
    }

    // is there an autoincrementing key?  if so, we have to add a returning parameter to grab it
    let returningData = _.reduce(collection.definition, (memo, attributes, field) => {
      if (attributes.autoIncrement === true) {
        memo.params.push({
          type: oracledb[utils.sqlTypeCast(attributes.type)],
          dir: oracledb.BIND_OUT
        })
        memo.fields.push(`"${field}"`)
        memo.outfields.push(`:${field}`)
        memo.rawFields.push(field)
      }
      return memo
    }, {
      params: [],
      fields: [],
      rawFields: [],
      outfields: []
    })

    let queryObj = {}

    if (returningData.params.length > 0) {
      query.query += ' RETURNING ' + returningData.fields.join(', ') + ' INTO ' + returningData.outfields.join(', ')
      query.values = query.values.concat(returningData.params)
      queryObj.outFormat = oracledb.OBJECT
    }

    queryObj.sql = query.query
    queryObj.params = query.values

    this.executeQuery(connectionName, queryObj)
      .then((result) => {
        // join the returning fields with the out params
        returningData.rawFields.forEach((field, index) => {
          data[field] = result.outBinds[index][0]
        })
        cb(null, data)
      })
      .catch(cb)
  },

  find(connectionName, collectionName, options, cb, connection) {
    const connectionObject = this.connections.get(connectionName)
    const collection = connectionObject.collections[collectionName]

    let schema = collection.waterline.schema
    let sequel = new Sequel(schema, this.sqlOptions)
    let query

    // Build a query for the specific query strategy
    try {
      query = sequel.find(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    this.executeQuery(connectionName, {
        sql: query.query[0],
        params: query.values[0]
      })
      .then((results) => {
        cb(null, results && results.rows)
      })
      .catch(cb)

  },

  destroy(connectionName, collectionName, options, cb, connection) {
    const connectionObject = this.connections.get(connectionName)
    const collection = connectionObject.collections[collectionName]

    // Build query
    const schema = collection.waterline.schema;
    let query;
    const sequel = new Sequel(schema, this.sqlOptions);

    // Build a query for the specific query strategy
    try {
      query = sequel.destroy(collectionName, options);
    } catch (e) {
      return cb(e);
    }

    this.find(connectionName, collectionName, options, (err, res) => {
      this.executeQuery(connectionName, {
        sql: query.query,
        params: query.values
      }).nodeify(cb)

    }, connection);
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

    return this.executeQuery(connectionName, queries).nodeify(cb)
  },

  update(connectionName, collectionName, options, values, cb, connection) {
    //    var processor = new Processor();
    const connectionObject = this.connections.get(connectionName);
    const collection = connectionObject.collections[collectionName];

    // Build find query
    const schema = collection.waterline.schema;
    const sequel = new Sequel(schema, this.sqlOptions);

    /*
        // Build a query for the specific query strategy
        try {
          //_query = sequel.find(collectionName, lodash.cloneDeep(options));
          _query = sequel.find(collectionName, _.clone(options));
        } catch (e) {
          return cb(e);
        }

        execQuery(connections[connectionName], _query.query[0], [], function(err, results) {
          if (err) {
            if (LOG_ERRORS) {
              console.log("#Error executing Find_1 (Update) " + err.toString() + ".");
            }
            return cb(err);
          }
          var ids = [];

          var pk = 'id';
          Object.keys(collection.definition).forEach(function(key) {
            if (!collection.definition[key].hasOwnProperty('primaryKey'))
              return;
            pk = key;
          });
          // update statement will affect 0 rows
          if (results.length === 0) {
            return cb(null, []);
          }

          results.forEach(function(result) {
            //ids.push(result[pk.toUpperCase()]);
            ids.push(result[pk]);
          });

          // Prepare values
          Object.keys(values).forEach(function(value) {
            values[value] = utils.prepareValue(values[value]);
          });

          var definition = collection.definition;
          var attrs = collection.attributes;

          Object.keys(definition).forEach(function(columnName) {
            var column = definition[columnName];

            if (fieldIsDatetime(column)) {
              if (!values[columnName])
                return;
              values[columnName] = SqlString.dateField(values[columnName]);
            } else if (fieldIsBoolean(column)) {
              values[columnName] = (values[columnName]) ? 1 : 0;
            }
          });
    */
    let query
      // Build query
    try {
      query = sequel.update(collectionName, options, values);
    } catch (e) {
      return cb(e);
    }

    //console.log('the query!', query)

    // Run query
    this.executeQuery(connectionName, {
      sql: query.query,
      params: query.values
    })
    .then((result) => {
      //console.log('update result', result)
      cb(null, result)
    })
    .catch((err) => {
        console.log('err', err)
        cb(err)
    })

  }
}

export default adapter
