import oracledb from 'oracledb'
  /**
   * Utility Functions
   */

// Dependencies
const _ = require('lodash')
const quoteRe = /^"(.*)"$/

// Module Exports

let utils = module.exports = {};

/**
 * Marshall a Config Object into a PG connection object
 */

utils.marshalConfig = function(config) {
  return _.extend(config, {
    host: config.host,
    user: config.user,
    password: config.password,
    database: config.database,
    port: config.port
  });
};

/**
 * Safe hasOwnProperty
 */

utils.object = {};

/**
 * Safer helper for hasOwnProperty checks
 *
 * @param {Object} obj
 * @param {String} prop
 * @return {Boolean}
 * @api public
 */

var hop = Object.prototype.hasOwnProperty;
utils.object.hasOwnProperty = function(obj, prop) {
  return hop.call(obj, prop);
};

/**
 * Escape Name
 *
 * Wraps a name in quotes to allow reserved
 * words as table or column names such as user.
 */

function escapeName(name) {
  return '"' + name + '"';
}
utils.escapeName = escapeName;

/**
 * Build a schema from an attributes object
 */
utils.buildSchema = function(obj) {
  var columns = _.map(obj, function(attribute, name) {
    if (_.isString(attribute)) {
      var val = attribute;
      attribute = {};
      attribute.type = val;
    }

    var type = utils.sqlTypeCast(attribute.autoIncrement ? 'NUMBER' : attribute.type);
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && !attribute.primaryKey && 'UNIQUE';

    return _.compact(['"' + name + '"', type, nullable, unique]).join(' ');
  }).join(',');

  var primaryKeys = _.keys(_.pick(obj, function(attribute) {
    return attribute.primaryKey;
  }));

  var constraints = _.compact([
    primaryKeys.length && 'PRIMARY KEY ("' + primaryKeys.join('","') + '")'
  ]).join(', ');

  return _.compact([columns, constraints]).join(', ');
};

/**
 * Build an Index array from any attributes that
 * have an index key set.
 */

utils.buildIndexes = function(obj) {
  var indexes = [];

  // Iterate through the Object keys and pull out any index attributes
  Object.keys(obj).forEach(function(key) {
    if (obj[key].hasOwnProperty('index')) {
      indexes.push(key);
    }
  });

  return indexes;
};


/**
 * Map Attributes
 *
 * Takes a js object and creates arrays used for parameterized
 * queries in postgres.
 */

utils.mapAttributes = function(data) {
  var keys = [], // Column Names
    values = [], // Column Values
    params = [], // Param Index, ex: $1, $2
    i = 1;

  Object.keys(data).forEach(function(key) {
    keys.push('"' + key + '"');
    values.push(utils.prepareValue(data[key]));
    params.push('$' + i);
    i++;
  });

  return ({
    keys: keys,
    values: values,
    params: params
  });
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value) {

  if (!value) return value

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays and Objects as strings
  if (Array.isArray(value) || value.constructor && value.constructor.name === 'Object') {
    try {
      value = JSON.stringify(value);
    } catch (e) {
      // just keep the value and let the db handle an error
      value = value;
    }
  }

  // Store Buffers as hex strings (for BYTEA)
  if (Buffer.isBuffer(value)) {
    //value = `X'${value.toString('hex')}'`;
    value = value.toString('utf8') 
  }

  return value;
};

/**
 * Normalize a schema for use with Waterline
 */
utils.normalizeSchema = function(schema, definition) {

  return _.reduce(schema.rows, function(memo, field) {
    // console.log('definition normalize');console.log(definition);
    var attrName = field.COLUMN_NAME.toLowerCase();

    Object.keys(definition).forEach(function(key) {
      if (attrName === key.toLowerCase()) attrName = key;
    });
    var type = field.DATA_TYPE;

    // Remove (n) column-size indicators
    type = type.replace(/\([0-9]+\)$/, '');

    memo[attrName] = {
      type: type
        // defaultsTo: '',
        //autoIncrement: field.Extra === 'auto_increment'
    };

    if (field.primaryKey) {
      memo[attrName].primaryKey = field.primaryKey;
    }

    if (field.unique) {
      memo[attrName].unique = field.unique;
    }

    if (field.indexed) {
      memo[attrName].indexed = field.indexed;
    }
    return memo;
  }, {});
}






/**
 * JS Date to UTC Timestamp
 *
 * Dates should be stored in Postgres with UTC timestamps
 * and then converted to local time on the client.
 */

utils.toSqlDate = function(date) {
  return date.toUTCString();
};

/**
 * Cast waterline types to Postgresql data types
 */

utils.sqlTypeCast = function(type) {
  switch (type.toLowerCase()) {
    case 'json':
    case 'array':
    case 'string':
    case 'text':
    case 'mediumtext':
    case 'longtext':
      return 'VARCHAR2(255)'

    case 'boolean':
    case 'int':
    case 'integer':
    case 'number':
    case 'smallint':
    case 'bigint':
    case 'real':
    case 'float':
    case 'double':
    case 'decimal':
      return 'NUMBER'

      // Store all time with the time zone
      // Store all dates as timestamps with the time zone
    case 'date':
    case 'time':
    case 'datestamp':
    case 'datetime':
      return 'DATE'

    case 'binary':
      return 'VARCHAR2(3000)'

    default:
      console.error("Unregistered type given: " + type);
      return "VARCHAR2(255)";
  }
};

utils.alterParameterSymbol = (query, symbol = ':') => {
  let re = /\$/g
  return query.replace(re, symbol)
}

utils.getAutoIncrementFields = (definition) => {
  return _.compact(_.map(definition, (value, key) => {
    if (value.autoIncrement) {
      return key
    }
  }))
}

utils.getSequenceName = (collectionName, fieldName) => {
  return `${collectionName}_${fieldName}_seq`
}

// get all return data
utils.getReturningData = (definition) => {

  return _.reduce(definition, (memo, attributes, field) => {
    memo.params.push({
      type: oracledb[utils.sqlTypeCast(attributes.type)],
      dir: oracledb.BIND_OUT
    })
    memo.fields.push(`"${field}"`)
    memo.outfields.push(`:${field}`)
    memo.rawFields.push(field)
    return memo
  }, {
    params: [],
    fields: [],
    rawFields: [],
    outfields: []
  })

}

utils.transformBulkOutbinds = (results, fields) => {
  if (_.isEmpty(results) || _.isEmpty(fields)) {
    return
  }
  let outbinds = _.cloneDeep(results)

  let objs = []
  const length = outbinds[0].length
  outbinds[0].forEach(() => {
    objs.push({})
  })

  fields = fields.map((field) => {
    return field.replace(quoteRe, "$1")
  })

  for (let i = 0; i < length; i++) {
    let obj = objs[i]
    outbinds.forEach((column, index) => {
      obj[fields[index]] = column.pop()
    })
  }
  return objs
}
