/* sqb-connect-pg
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://sqbjs.github.io/sqb-connect-pg/
 */

/**
 * Module dependencies.
 * @private
 */
const EventEmitter = require('events');

class PgCursor extends EventEmitter {
  /**
   *
   * @param {Object} cursor
   * @param {Object} options
   * @constructor
   */
  constructor(cursor, options) {
    super();
    this._cursor = cursor;
    this._cache = [];
    this._prefetchRows = options.fetchRows || 100;
  }

  get isClosed() {
    return !this._cursor;
  }

  close() {
    if (!this._cursor)
      return Promise.resolve();
    return new Promise((resolve, reject) => {
      this._cursor.close(err => {
        if (err)
          return reject(err);
        this._cursor = undefined;
        this.emit('close');
        resolve();
      });
    });
  }

  fetch(nRows) {
    if (this.isClosed)
      return Promise.resolve();

    return new Promise((resolve, reject) => {
      /* Copy from cache if exists */
      const crows = Math.min(nRows, this._cache.length);
      const result = crows ? this._cache.slice(0, crows) : [];
      let needRows = nRows - result.length;

      if (this._eof) {
        // It is safe to remove rows from cache that previously added to result
        if (result.length)
          this._cache.splice(0, result.length);
        if (result.length)
          return resolve(result);
        return resolve();
      }

      /* Read rows from cursor if cache rows count is lover than %10 */
      //const rowsToRead = needRows || this._cache.length < this._prefetchRows / 10;
      if (needRows || this._cache.length < this._prefetchRows / 10) {
        const rowsToRead = Math.max(needRows, this._prefetchRows);
        this._cursor.read(rowsToRead, (err, rows, response) => {
          if (err)
            return reject(err);

          this._fields = response && response.fields;

          // It is safe to remove rows from cache that previously added to result
          if (result.length)
            this._cache.splice(0, result.length);

          this._eof = !rows || rows.length < rowsToRead;
          if (rows && rows.length) {
            /* Remove "anonymous" prototype from rows */
            if (!Array.isArray(rows[0]))
              for (const row of rows) {
                Object.setPrototypeOf(row, Object.prototype);
              }
            // Move rows to result array
            if (needRows)
              result.push(...rows.splice(0, needRows));

            // Move excess records to cache
            if (rows.length)
              this._cache.push(...rows);
          }

          if (result.length)
            return resolve(result);
          resolve();
        });
        return;
      }

      /* Remove rows from cache, that previously moved to result */
      this._cache.splice(0, result.length);
      resolve(result);
    });
  }

}

/**
 * Expose `PgCursor`.
 */
module.exports = PgCursor;
