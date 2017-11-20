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
const pg = require('pg');
const waterfall = require('putil-waterfall');
const PgConnection = require('./connection');
const PgMetaOperator = require('./metaoperator');

const PARAMTYPE_DOLLAR = 2;

/**
 * Expose `PgDriver`.
 */

module.exports = PgDriver;

function PgDriver(config) {
  // Sql injection check
  if (config.schema && !config.schema.match(/^\w+$/))
    throw new Error('Invalid schema name');
  this.config = config || {};
  this.metaOperator = new PgMetaOperator();
  this.paramType = PARAMTYPE_DOLLAR;
}

const proto = PgDriver.prototype;

proto.createConnection = function(callback) {
  const config = this.config;
  const self = this;
  var connection;
  var client;
  waterfall([
        // Create client
        function(next) {
          const Client = self.config.native ? pg.native.Client : pg.Client;
          client = new Client({
            connectionString: ('postgresql://' + (config.user || '') + ':' +
                (config.password || '') + '@' + config.database),
            client_encoding: config.encoding
          });
          next();
        },
        // Establish connection
        function(next) {
          client.connect(next);
        },
        // Create connection and set default schema
        function(next) {
          connection = new PgConnection(client);
          connection.sessionId = client.processID;
          if (config.schema) {
            client.query('SET search_path = ' + schema, next);
          } else next();
        }
      ],

      function(err) {
        if (err && connection) {
          return client.end(function(err2) {
            callback(err || err2);
          });
        }
        callback(err, connection);
      });
};
