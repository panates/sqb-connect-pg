/* sqb-connect-pg
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-pg/
 */

const waterfall = require('putil-waterfall');

/**
 * Expose `PgMetaOperator`.
 */
module.exports = PgMetaOperator;

/**
 * @param {Object} sqbObj
 * @constructor
 */
function PgMetaOperator(sqbObj) {
}

const proto = PgMetaOperator.prototype;

proto.querySchemas = function(sqb) {
  return sqb
      .select('schema_name')
      .from('information_schema.schemata');
};

proto.queryTables = function(sqb) {
  return sqb
      .select('t.table_schema schema_name', 't.table_name', 't.commit_action',
          sqb.select(
              sqb.raw('(select pg_catalog.obj_description(c.oid))')
          ).from('pg_catalog.pg_class c')
              .where(['c.relname', sqb.raw('t.table_name')])
              .as('table_comments')
      )
      .from('information_schema.tables t')
      .where(['table_type', 'BASE TABLE'])
      .orderBy('t.table_schema', 't.table_name');
};

proto.queryColumns = function(sqb) {
  const query = sqb
      .select('t.table_schema schema_name', 't.table_name', 't.column_name',
          't.ordinal_position column_position', 't.is_nullable nullable',
          't.data_type', 't.data_type data_type_mean', 't.udt_name',
          't.character_maximum_length char_length',
          't.numeric_precision data_precision', 't.numeric_scale data_scale',
          't.domain_schema', 't.domain_name',
          sqb.select(
              sqb.raw('(select pg_catalog.col_description(c.oid, t.ordinal_position))')
          ).from('pg_catalog.pg_class c')
              .where(['c.relname', sqb.raw('t.table_name')])
              .as('column_comments')
      )
      .from('information_schema.columns t')
      .orderBy('t.table_schema', 't.table_name', 't.ordinal_position');
  query.on('fetch', function(row) {
    switch (row.udt_name) {
      case 'int2':
        row.set('data_type_mean', 'SMALLINT');
        break;
      case 'int4':
        row.set('data_type_mean', 'INTEGER');
        break;
      case 'int8':
        row.set('data_type_mean', 'BIGINT');
        break;
      case 'float4':
        row.set('data_type_mean', 'FLOAT');
        break;
      case 'float8':
        row.set('data_type_mean', 'DOUBLE');
        break;
      case 'numeric':
        row.set('data_type_mean', 'NUMBER');
        break;
      case 'bpchar':
      case 'text':
        row.set('data_type_mean', 'VARCHAR');
        break;
      case 'bytea':
        row.set('data_type_mean', 'BUFFER');
        break;
    }
  });
  return query;
};

proto.queryPrimaryKeys = function(db) {
  return db
      .select('t.constraint_schema', 't.table_name', 't.constraint_name',
          db.raw('string_agg(u.column_name, \',\') column_names')
      )
      .from('information_schema.table_constraints t')
      .join(
          db.join('information_schema.constraint_column_usage u')
              .on(['u.constraint_catalog', db.raw('t.constraint_catalog')],
                  ['u.constraint_schema', db.raw('t.constraint_schema')],
                  ['u.constraint_name', db.raw('t.constraint_name')]
              )
      ).where(['t.constraint_type', 'PRIMARY KEY'])
      .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name');
};

proto.queryForeignKeys = function(db) {
  return db
      .select('t.constraint_schema', 't.table_name', 't.constraint_name',
          'kcu.column_name',
          'ccu.constraint_catalog foreign_catalog',
          'ccu.constraint_schema foreign_schema',
          'ccu.table_name foreign_table_name',
          db.raw('string_agg(ccu.column_name, \',\') foreign_columns')
      )
      .from('information_schema.table_constraints t')
      .join(
          db.join('information_schema.key_column_usage kcu')
              .on(['kcu.constraint_catalog', db.raw('t.constraint_catalog')],
                  ['kcu.constraint_schema', db.raw('t.constraint_schema')],
                  ['kcu.constraint_name', db.raw('t.constraint_name')]
              ),
          db.join('information_schema.constraint_column_usage ccu')
              .on(['ccu.constraint_catalog', db.raw('t.constraint_catalog')],
                  ['ccu.constraint_schema', db.raw('t.constraint_schema')],
                  ['ccu.constraint_name', db.raw('t.constraint_name')]
              )
      ).where(['t.constraint_type', 'FOREIGN KEY'])
      .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name',
          't.table_name', 'kcu.column_name', 'ccu.constraint_catalog',
          'ccu.constraint_schema', 'ccu.table_name');
};

proto.getTableInfo = function(db, schema, tableName, callback) {
  const self = this;
  const result = {};
  Promise.all([
    /* Columns resolver */
    new Promise(function(resolve, reject) {
      const query = self.queryColumns(db)
          .where(['t.table_schema', schema], ['t.table_name', tableName]);
      query.execute({
        fetchRows: 100000,
        objectRows: true,
        naming: 'lowercase'
      }, function(err, resp) {
        if (err)
          return reject(err);
        result.columns = {};
        resp.rows.forEach(function(row, i) {
          row = result.columns[row.column_name] =
              Object.assign({column_index: i}, row);
          delete row.schema_name;
          delete row.table_name;
          delete row.column_name;
        });
        resolve();
      });
    }),
    /* Primary key resolver */
    new Promise(function(resolve, reject) {
      const query = self.queryPrimaryKeys(db)
          .where(['t.table_schema', schema], ['t.table_name', tableName]);
      query.execute({
        fetchRows: 100000,
        objectRows: true,
        naming: 'lowercase'
      }, function(err, resp) {
        if (err)
          return reject(err);
        if (resp.rows.length) {
          const row = result.primaryKey = Object.assign({}, resp.rows[0]);
          delete row.schema_name;
          delete row.table_name;
        }
        resolve();
      });
    }),
    /* Foreign keys resolver */
    new Promise(function(resolve, reject) {
      const query = self.queryForeignKeys(db)
          .where(['t.table_schema', schema], ['t.table_name', tableName]);
      query.execute({
        fetchRows: 100000,
        objectRows: true,
        naming: 'lowercase'
      }, function(err, resp) {
        if (err)
          return reject(err);
        if (resp.rows.length) {
          result.foreignKeys = [];
          resp.rows.forEach(function(row) {
            row = Object.assign({}, row);
            delete row.schema_name;
            delete row.table_name;
            result.foreignKeys.push(row);
          });
        }
        resolve();
      });
    })
  ]).then(function() {
    callback(null, result);
  }).catch(callback);
};
