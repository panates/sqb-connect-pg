const fs = require('fs');
const path = require('path');
const waterfall = require('putil-waterfall');

function createTestTables(client, schemaName) {

  const jsons = [];

  return waterfall([

        /* Create sqb_test schema */
        (next) => {
          client.query('CREATE SCHEMA ' + schemaName + ' AUTHORIZATION postgres;',
              (err) => {
                if (!err || err.message.indexOf('already exists'))
                  return next();
                next(err);
              });
        },

        /* Load jsons */
        (done) => {
          const dir = path.join(__dirname, 'data');
          const files = fs.readdirSync(dir);
          for (const f of files) {
            const j = require(path.join(dir, f));
            jsons.push(j);
          }
          done();
        },

        /* Drop tables */
        () => {
          const reversed = jsons.slice().reverse();
          return waterfall.every(reversed, (next, table) => {
            client.query({text: 'drop table ' + table.name},
                (err) => {
                  if (!err || err.message.indexOf('not exists'))
                    return next();
                  next(err);
                });
          });
        },

        /* Create tables */
        () => {
          /* Iterate every table */
          return waterfall.every(jsons, (next, table) => {

            return waterfall([
              /* Create table */
              (next) => client.query(table.createSql, (err) => {
                if (err) {
                  err.message =
                      'Can not create table "' + table.name + '". ' + err.message;
                  return next(err);
                }
                next();
              }),

              /* Insert rows */
              () => {
                const fieldKeys = Object.getOwnPropertyNames(table.rows[0]);
                let s1 = '';
                let s2 = '';
                for (const [i, f] of fieldKeys.entries()) {
                  s1 += (i ? ',' : '') + f.toLowerCase();
                  s2 += (i ? ',$' : '$') + (i + 1);
                }
                const insertSql = 'insert into ' + table.name + ' (' + s1 +
                    ') values (' + s2 + ')';
                return waterfall.every(table.rows, (next, row) => {
                  const params = [];
                  for (const key of fieldKeys) {
                    params.push(row[key] == null ? null : row[key]);
                  }
                  client.query({
                    text: insertSql,
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
