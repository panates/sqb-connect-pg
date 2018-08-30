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
const PgConnection = require('./PgConnection');

class PgAdapter {
  constructor(config) {
    // Sql injection check
    if (config.schema && !config.schema.match(/^\w+$/))
      throw new Error('Invalid schema name');
    this.config = config || {};
    this.paramType = 2; // DOLLAR
  }

  /**
   * @return {Promise<PgConnection>}
   */
  createConnection() {
    const config = this.config;
    return new Promise((resolve, reject) => {
      /* Create client */
      const Client = config.native ? pg.native.Client : pg.Client;
      const connectionString = 'postgresql://' +
          (config.connectString ? config.connectString :
                  (config.user || '') + ':' + (config.password || '') +
                  '@' + (config.host || 'localhost') +
                  (config.database ? '/' + config.database : '')
          );
      const client = new Client({
        connectionString: connectionString,
        client_encoding: config.encoding
      });

      let connection;
      waterfall([
            /* Establish connection */
            (next) => client.connect(next),

            // Create connection and set default schema
            (next) => {
              connection = new PgConnection(client);
              connection.sessionId = client.processID;
              if (config.schema) {
                client.query('SET search_path = ' + config.schema, next);
              } else next();
            }
          ],

          function(err) {
            if (err) {
              if (connection)
                return client.end((err2) => reject(err || err2));
              return reject(err);
            }
            resolve(connection);
          });
    });
  };

}

/**
 * Expose `PgAdapter`.
 */
module.exports = PgAdapter;