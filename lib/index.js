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
const PgAdapter = require('./PgAdapter');
const PgMetaOperator = require('./PgMetaOperator');

module.exports = Object.assign({}, require('sqb-serializer-pg'));

module.exports.createAdapter = function(config) {
  /* istanbul ignore else */
  if (config.dialect === 'pg' || config.dialect === 'postgres') {
    return new PgAdapter(config);
  }
};

module.exports.createMetaOperator = function(config) {
  /* istanbul ignore else */
  if (config.dialect === 'pg' || config.dialect === 'postgres') {
    return new PgMetaOperator();
  }
};
