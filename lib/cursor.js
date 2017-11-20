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

/**
 * Expose `PgCursor`.
 */
module.exports = PgCursor;

/**
 *
 * @param {Object} cursor
 * @param {Object} options
 * @constructor
 */

function PgCursor(cursor, options) {
  this._cursor = cursor;
  this._cache = [];
  this._prefetchRows = options.fetchRows || 100;
}

const proto = PgCursor.prototype = {

  get bidirectional() {
    return false;
  },

  get isClosed() {
    return !this._cursor;
  }

};
proto.constructor = PgCursor;

proto.close = function(callback) {
  const self = this;
  if (self._cursor)
    self._cursor.close(function(err) {
      if (!err)
        self._cursor = undefined;
      callback(err);
    });
  else callback();
};

proto.fetch = function(nRows, callback) {

  if (this.isClosed)
    return callback();

  const self = this;
  /* Copy from cache if exists */
  const crows = Math.min(nRows, self._cache.length);
  const result = crows ? self._cache.slice(0, crows) : [];
  const needRows = nRows - result.length;

  if (self._eof) {
    // It is safe to remove rows from cache that previously added to result
    if (result.length)
      self._cache.splice(0, result.length);
    if (result.length)
      callback(undefined, result);
    else
      callback();
    return;
  }

  /* Read rows from cursor if there was not enough in cache or
   * to fill cache if cache rows is lover than %10 */
  if (needRows || self._cache.length < self._prefetchRows / 10) {
    const rowsToRead = needRows + self._prefetchRows;
    self._cursor.read(rowsToRead, function(err, rows, response) {
      if (err)
        return callback(err);
      self._fields = response && response.fields;

      // It is safe to remove rows from cache that previously added to result
      if (result.length)
        self._cache.splice(0, result.length);

      self._eof = !rows || rows.length < rowsToRead;
      if (rows && rows.length) {
        if (needRows)
          Array.prototype.push.apply(result, rows.splice(0, needRows));
        // Move excess records to cache
        if (rows.length)
          Array.prototype.push.apply(self._cache, rows);
      }

      if (result.length)
        callback(undefined, result);
      else
        callback();
    });
    return;
  }

  /* Remove rows from cache, that previously moved to result */
  self._cache.splice(0, result.length);
  callback(undefined, result);
};
