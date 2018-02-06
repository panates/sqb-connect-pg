/* sqb-connect-pg
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-pg/
 */

/**
 * Module dependencies.
 * @private
 */
const PgCursor = require('./PgCursor');
const Cursor = require('pg-cursor');
const waterfall = require('putil-waterfall');

/**
 * Expose `PgConnection`.
 */
module.exports = PgConnection;

/**
 *
 * @param {Object} intlcon
 * @constructor
 */
function PgConnection(intlcon) {
  this.intlcon = intlcon;
}

const proto = PgConnection.prototype = {
  /**
   * @override
   * @return {boolean}
   */
  get isClosed() {
    return !this.intlcon;
  }
};
proto.constructor = PgConnection;

proto.close = function(callback) {
  if (this.intlcon) {
    const conn = this.intlcon;
    this.intlcon = undefined;
    conn.end();
    callback();
  }
};

proto.execute = function(sql, params, options, callback) {
  if (this.isClosed) {
    callback(new Error('Can not execute while connection is closed'));
    return;
  }
  const self = this;

  const convertFields = function(fields) {
    result = [];
    fields.forEach(function(v, idx) {
      const o = {
        index: idx,
        name: v.name,
        dataType: fetchTypeMap[v.dataTypeID] || 'String',
        fieldType: dbTypeMap[v.dataTypeID] || v.dataTypeID
      };
      if (v.dataTypeID === 18)
        o.fixedLength = true;
      if (v.dataTypeSize && v.dataTypeSize > 0)
        o.size = v.dataTypeSize;
      result.push(o);
    });
    return result;
  };

  if (options.cursor) {
    // Crete cursor
    const cursor = new PgCursor(
        self.intlcon.query(
            new Cursor(sql, params || [],
                {rowMode: !options.objectRows ? 'array' : null})
        ),
        options);
    // Execute query and prefetch rows
    cursor.fetch(0, function(err) {
      if (err)
        return callback(err);
      callback(undefined, {
        cursor: cursor,
        fields: convertFields(cursor._fields)
      });
    });
    return;
  }

  const pgQuery = {
    text: sql,
    values: params || [],
    rowMode: !options.objectRows ? 'array' : null
  };
  if (options.name)
    pgQuery.name = options.name;
  if (!options.cursor) {
    pgQuery.rows = options.fetchRows;
    pgQuery.portal = 'my favorite portal';
  }

  self.intlcon.query(pgQuery, function(err, response) {
    if (err)
      return callback(err);
    //console.log(response);
    const result = {command: response.command};
    // Create array of field metadata
    if (response.fields)
      result.fields = convertFields(response.fields);

    if (response.rows) {
      if (response.rows.length) {
        /* Remove "anonymous" prototype from rows */
        if (!Array.isArray(response.rows[0]))
          response.rows.forEach(function(row) {
            Object.setPrototypeOf(row, Object.prototype);
          });
      }
      result.rows = response.rows;
    }

    callback(undefined, result);
  });
};

proto.startTransaction = function(callback) {
  this.intlcon.query('BEGIN', callback);
};

proto.commit = function(callback) {
  this.intlcon.query('COMMIT', callback);
};

proto.rollback = function(callback) {
  this.intlcon.query('ROLLBACK', callback);
};

proto.test = function(callback) {
  const self = this;
  self.intlcon.query('select 1', [], function(err) {
    callback(err);
  });
};

const fetchTypeMap = {
  16: 'Boolean',
  17: 'Buffer',
  20: 'String',   // int8
  21: 'Number',   // int2
  23: 'Number',   // int4
  26: 'Number',   // oid
  700: 'Number',  // float4/real
  701: 'Number',  // float8/double
  1082: 'Date',   // date
  1114: 'Date',   // timestamp without timezone
  1184: 'Date',   // timestamp
  114: 'Object',  // json
  600: 'Object',  // point
  718: 'Object',  // circle
  3802: 'Object', // jsonb
  199: 'Array',
  1000: 'Array',
  1001: 'Array',
  1005: 'Array',
  1007: 'Array',
  1016: 'Array',
  1017: 'Array',
  1021: 'Array',
  1022: 'Array',
  1028: 'Array',
  1231: 'Array',
  1115: 'Array',
  1182: 'Array',
  1185: 'Array',
  3807: 'Array'
};

const dbTypeMap = {
  16: 'bool',
  17: 'bytea',
  18: 'char',
  19: 'name',
  20: 'int8',
  21: 'int2',
  23: 'int4',
  24: 'regproc',
  25: 'text',
  26: 'oid',
  27: 'tid',
  28: 'xid',
  29: 'cid',
  142: 'xml',
  600: 'point',
  601: 'lseg',
  602: 'path',
  603: 'box',
  604: 'polygon',
  628: 'line',
  650: 'cidr',
  700: 'float4',
  701: 'float8',
  702: 'abstime',
  703: 'reltime',
  704: 'tinterval',
  705: 'unknown',
  718: 'circle',
  790: 'money',
  829: 'macaddr',
  869: 'inet',
  1042: 'bpchar',
  1043: 'varchar',
  1082: 'date',
  1083: 'time',
  1114: 'timestamp',
  1184: 'timestamptz',
  1186: 'interval',
  1266: 'timetz',
  1560: 'bit',
  1562: 'varbit',
  1700: 'numeric',
  1790: 'refcursor',
  2249: 'record',
  2278: 'void'
};
