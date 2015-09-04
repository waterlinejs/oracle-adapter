/**
 * Utility Functions
 */

// Dependencies
const _ = require('lodash');

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
  return '"' + name.toUpperCase() + '"';
}
utils.escapeName = escapeName;

/**
 * Build a schema from an attributes object
 */
utils.buildSchema = function(obj) {
  var columns = _.map(obj, function (attribute, name) {
    if (_.isString(attribute)) {
      var val = attribute;
      attribute = {};
      attribute.type = val;
    }

    var type = utils.sqlTypeCast(attribute.autoIncrement ? 'NUMBER' : attribute.type);
    var nullable = attribute.notNull && 'NOT NULL';
    var unique = attribute.unique && 'UNIQUE';

    return _.compact([ '"' + name + '"', type, nullable, unique ]).join(' ');
  }).join(',');

  var primaryKeys = _.keys(_.pick(obj, function (attribute) {
    return attribute.primaryKey;
  }));

  var constraints = _.compact([
    primaryKeys.length && 'PRIMARY KEY ("' + primaryKeys.join('","') + '")'
  ]).join(', ');

  return _.compact([ columns, constraints ]).join(', ');
};

/**
 * Build an Index array from any attributes that
 * have an index key set.
 */

utils.buildIndexes = function(obj) {
  var indexes = [];

  // Iterate through the Object keys and pull out any index attributes
  Object.keys(obj).forEach(function(key) {
    if(obj[key].hasOwnProperty('index')) {
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
  var keys = [],   // Column Names
      values = [], // Column Values
      params = [], // Param Index, ex: $1, $2
      i = 1;

  Object.keys(data).forEach(function(key) {
    keys.push('"' + key + '"');
    values.push(utils.prepareValue(data[key]));
    params.push('$' + i);
    i++;
  });

  return({ keys: keys, values: values, params: params });
};

/**
 * Prepare values
 *
 * Transform a JS date to SQL date and functions
 * to strings.
 */

utils.prepareValue = function(value) {

  // Cast dates to SQL
  if (_.isDate(value)) {
    value = utils.toSqlDate(value);
  }

  // Cast functions to strings
  if (_.isFunction(value)) {
    value = value.toString();
  }

  // Store Arrays as strings
  if (Array.isArray(value)) {
    value = JSON.stringify(value);
  }

  // Store Buffers as hex strings (for BYTEA)
  if (Buffer.isBuffer(value)) {
    value = '\\x' + value.toString('hex');
  }

  return value;
};

/**
 * Normalize a schema for use with Waterline
 */
utils.normalizeSchema = function(schema) {
  let normalized = {}
  //TODO throw here?
  if (!schema) return normalized

  let clone = _.clone(schema.rows);

  clone.forEach((column) => {

    // Set Type
    normalized[column[0]] = {
      type: column[1]
    }

    // Check for Primary Key
    /*
    if(column.Constraint && column.C === 'p') {
      normalized[column.Column].primaryKey = true;
    }
    */

    // Check for Unique Constraint
    if(column[2] === 'N') {
      normalized[column[0]].unique = true;
    }

    // Check for autoIncrement
    /*
    if(column.autoIncrement) {
      normalized[column.Column].autoIncrement = column.autoIncrement;
    }
    */

    // Check for index
    /*
    if(column.indexed) {
      normalized[column.Column].indexed = column.indexed;
    }
    */

  })

  return normalized;
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
    case 'string':
    case 'text':
    case 'mediumtext':
    case 'longtext':
      return 'VARCHAR2(64)';

    case 'boolean':
      return 'BOOLEAN';

    case 'int':
    case 'integer':
    case 'number':
    case 'smallint':
    case 'bigint':
      return 'NUMBER';

    case 'real':
    case 'float':
    case 'double':
    case 'decimal':
      return 'FLOAT';

    // Store all time with the time zone
    case 'time':
        return 'TIME WITH TIME ZONE';
    // Store all dates as timestamps with the time zone
    case 'date':
      return 'DATE';
    case 'datestamp':
    case 'datetime':
      return 'TIMESTAMP WITH TIME ZONE';

    default:
      console.error("Unregistered type given: " + type);
      return "VARCHAR2(64)";
  }
};

utils.alterParameterSymbol = (query, symbol=':') => {
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

