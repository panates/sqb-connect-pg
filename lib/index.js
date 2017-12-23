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
const PgDriver = require('./PgDriver');
const PgMetaOperator = require('./PgMetaOperator');

module.exports = {

  createSerializer: require('sqb-serializer-pg').createSerializer,

  createDriver: function(config) {
    if (config.dialect === 'pg' || config.dialect === 'postgres') {
      return new PgDriver(config);
    }
  },

  createMetaOperator: function(config) {
    if (config.dialect === 'pg' || config.dialect === 'postgres') {
      return new PgMetaOperator();
    }
  }

};
