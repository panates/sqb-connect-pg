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
 *
 * @param {Object} intlcon
 * @constructor
 */
class PgConnection {
  constructor(intlcon) {
    this.intlcon = intlcon;
  }

  /**
   * @override
   * @return {boolean}
   */
  get isClosed() {
    return !this.intlcon;
  }

  close() {
    if (!this.intlcon)
      return Promise.resolve();

    return new Promise((resolve, reject) => {
      this.intlcon.end((err) => {
        if (err)
          return reject(err);
        this.intlcon = null;
        this._deferredCommit = false;
        this._deferredRollback = false;
        this._cursorOpen = false;
        resolve();
      });
    });
  }

  /**
   *
   * @param {string} query
   * @param {string} query.sql
   * @param {Array} query.values
   * @param {Object} options
   * @param {Integer} [options.fetchRows]
   * @param {Boolean} [options.cursor]
   * @param {Boolean} [options.autoCommit]
   * @private
   * @return {Promise<Object>}
   */
  execute(query, options) {
    if (this.isClosed)
      return Promise.reject(new Error('Connection closed'));

    return waterfall([

      /* Start transaction if auto commit is off */
      (next) => {
        if (options.autoCommit)
          return next();
        return this.startTransaction();
      },

      /* Execute the query */
      () => this._execute(query, options),

      /* Commit transaction if auto commit is on */
      (next, result) => {
        if (!options.autoCommit)
          return next(null, result);
        return this.commit().then(() => result);
      }
    ]);
  }

  /**
   *
   * @param {string} query
   * @param {Object} options
   * @private
   * @return {Promise<Object>}
   */
  _execute(query, options) {

    if (options.cursor) {
      // Crete cursor
      const cursor = new PgCursor(
          this.intlcon.query(new Cursor(query.sql, query.params || [],
              {rowMode: !options.objectRows ? 'array' : null})
          ),
          options);
      this._cursorOpen = true;
      cursor.once('close', () => this._cursorClosed());
      // Execute query and prefetch rows
      return cursor.fetch(0).then(() => ({
        cursor: cursor,
        fields: convertFields(cursor._fields)
      }));
    }

    return new Promise((resolve, reject) => {
      const pgQuery = {
        text: query.sql,
        values: query.values || [],
        rowMode: !options.objectRows ? 'array' : null
      };
      if (options.name)
        pgQuery.name = options.action;
      if (!options.cursor && options.kind !== 'raw')
        pgQuery.rows = options.fetchRows;

      this.intlcon.query(pgQuery, (err, response) => {
        if (err)
          return reject(err);
        const result = {command: response.command};
        // Create array of field metadata
        if (response.fields)
          result.fields = convertFields(response.fields);
        if (['INSERT', 'UPDATE', 'DELETE'].includes(response.command) ||
            response.rowCount)
          result.rowsAffected = response.rowCount;
        if (response.rows) {
          /* Remove "anonymous" prototype from rows */
          if (response.rows.length && !Array.isArray(response.rows[0]))
            for (const row of response.rows) {
              Object.setPrototypeOf(row, Object.prototype);
            }
          result.rows = response.rows;
        }
        resolve(result);
      });
    });
  }

  startTransaction() {
    return new Promise((resolve, reject) => {
      this.intlcon.query('BEGIN', (err) => {
        if (err)
          return reject(err);
        resolve();
      });
    });
  }

  commit() {
    if (this._cursorOpen) {
      this._deferredCommit = true;
      this._deferredRollback = false;
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.intlcon.query('COMMIT;', (err) => {
        if (err)
          return reject(err);
        resolve();
      });
    });
  }

  rollback() {
    if (this._cursorOpen) {
      this._deferredCommit = false;
      this._deferredRollback = true;
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.intlcon.query('ROLLBACK', (err) => {
        if (err)
          return reject(err);
        resolve();
      });
    });
  }

  test() {
    return new Promise((resolve, reject) => {
      this.intlcon.query('select 1', [], (err) => {
        if (err)
          return reject(err);
        resolve();
      });
    });
  }

  _cursorClosed() {
    this._cursorOpen = false;
    try {
      if (this._deferredCommit)
        return this.commit();
      else if (this._deferredRollback)
        return this.rollback();
    } finally {
      this._deferredCommit = false;
      this._deferredRollback = false;
    }
  }

}

/**
 * Expose `PgConnection`.
 */
module.exports = PgConnection;

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

const convertFields = function(fields) {
  const result = [];
  for (const [idx, v] of fields.entries()) {
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
  }
  return result;
};
