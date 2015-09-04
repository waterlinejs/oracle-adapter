import oracledb from 'oracledb'
import utils from './utils'
import Promise from 'bluebird'
import Sequel from 'waterline-sequel'
import _ from 'lodash'

const adapter = {

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
      sql: `CREATE TABLE ${collectionName} (${utils.buildSchema(definition)})`
    }
    queries.push(query)

    // are there any fields that require auto increment?
    const autoIncrementFields = utils.getAutoIncrementFields(definition)
    if (autoIncrementFields.length > 0) {
        // create sequence and trigger queries for each one
      autoIncrementFields.forEach((field) => {
        let sequenceName = collectionName + '_seq'
          //queries.push({sql: `ALTER TABLE ${collectionName} ADD (CONSTRAINT ${collectionName}_pk PRIMARY KEY ("${field}"))`})
        queries.push({
          sql: `CREATE SEQUENCE ${sequenceName}`
        })
        let triggerSql = `CREATE OR REPLACE TRIGGER ${collectionName}_trg
                         BEFORE INSERT ON ${collectionName}
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
    collectionName = collectionName.toUpperCase()
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
      sql: `SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner
        FROM all_constraints cons, all_cons_columns cols WHERE cols.table_name = 
        :name AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = cols.owner
        ORDER BY cols.table_name, cols.position`,
      params: [collectionName]
    };

    return this.executeQuery(connectionName, queries)
      .spread((schema, indices, tablePrimaryKeys) => {
        cb(null, utils.normalizeSchema(schema))
      })
      .catch(cb)
  },

  executeQuery(connectionName, queries) {
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
              conn.execute(query.sql, query.params || [], options, (queryError, res) => {
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
    let tableName = schemaName + utils.escapeName(table).toUpperCase();

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

    var sequel = new Sequel(schema, {
      escapeInserts: true
    } /*options*/ );

    var incrementSequences = [];
    var query;

    // Build a query for the specific query strategy
    try {
      query = sequel.create(table.toUpperCase(), data);
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

    queryObj.sql = utils.alterParameterSymbol(query.query)
    queryObj.params = query.values

    this.executeQuery(connectionName, queryObj)
      .then((result) => {
          /*
           * results! { rowsAffected: 1,
           *   outBinds: [ [ 2 ] ],
           *     rows: undefined,
           *       metaData: undefined }
           */
        // join the returning fields with the out params
        returningData.rawFields.forEach((field, index) => {
            data[field] = result.outBinds[index][0]
        })
        cb(null, data)
      })
      .catch(cb)
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

}

export
default adapter
