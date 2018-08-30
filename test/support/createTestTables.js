const waterfall = require('putil-waterfall');
const tableRegions = require('./table_regions');
const tableAirports = require('./table_airports');

function createTestTables(client) {

  return waterfall([

        /* Drop stables */
        () => {
          return waterfall.every([tableAirports, tableRegions], (next, table) => {
            client.query({text: 'drop table ' + table.name},
                (err) => {
                  if (!err || err.message.indexOf('not exists'))
                    return next();
                  next(err);
                });
          });
        },

        /* Create sqb_test schema */
        (next) => {
          client.query('CREATE SCHEMA sqb_test AUTHORIZATION postgres;',
              (err) => {
                if (!err || err.message.indexOf('already exists'))
                  return next();
                next(err);
              });
        },

        /* Create tables */
        () => {
          /* Iterate every table */
          return waterfall.every([tableRegions, tableAirports],
              (next, table) => {

                return waterfall([
                  /* Create table */
                  (next) => client.query(table.createSql, next),

                  /* Insert rows */
                  () => {
                    const fieldKeys = Object.getOwnPropertyNames(table.rows[0]);
                    return waterfall.every(table.rows, (next, row) => {
                      const params = [];
                      for (const key of fieldKeys) {
                        params.push(row[key] || null);
                      }
                      client.query({
                        text: table.insertSql,
                        values: params
                      }, (err) => {
                        if (err)
                          return next(err);
                        next();
                      });
                    });
                  }

                ]);
              });

        }
      ]
  );
}

module.exports = createTestTables;