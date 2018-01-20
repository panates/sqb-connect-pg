/* sqb-connect-pg
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-pg/
 */

/**
 * Expose `PgMetaOperator`.
 */
module.exports = PgMetaOperator;

/**
 * @param {Object} sqbObj
 * @constructor
 */
function PgMetaOperator(sqbObj) {
  this.supportsSchemas = true;
}

const proto = PgMetaOperator.prototype;

proto.querySchemas = function(sqb) {
  return sqb
      .select('schema_name')
      .from('information_schema.schemata');
};

proto.queryTables = function(db) {
  const Op = db.Op;
  return db
      .select('t.table_schema schema_name', 't.table_name', 't.commit_action',
          db.select(
              db.raw('(select pg_catalog.obj_description(c.oid))')
          ).from('pg_catalog.pg_class c')
              .where(Op.eq('c.relname', db.raw('t.table_name')))
              .as('table_comments')
      )
      .from('information_schema.tables t')
      .where(Op.eq('table_type', 'BASE TABLE'))
      .orderBy('t.table_schema', 't.table_name');
};

proto.queryColumns = function(db) {
  const Op = db.Op;
  return db
      .select('t.table_schema schema_name', 't.table_name', 't.column_name',
          't.ordinal_position column_position',
          't.data_type',
          db.case()
              .when(Op.eq('udt_name', 'int2')).then('SMALLINT')
              .when(Op.eq('udt_name', 'int4')).then('INTEGER')
              .when(Op.eq('udt_name', 'int8')).then('BIGINT')
              .when(Op.eq('udt_name', 'float4')).then('FLOAT')
              .when(Op.eq('udt_name', 'float8')).then('DOUBLE')
              .when(Op.eq('udt_name', 'numeric')).then('NUMBER')
              .when(Op.in('udt_name', ['bpchar', 'text'])).then('NUMBER')
              .when(Op.eq('udt_name', 'numeric')).then('NUMBER')
              .else(db.raw('upper(t.udt_name)')).as('data_type_mean'),
          't.udt_name',
          't.character_maximum_length char_length',
          't.numeric_precision data_precision', 't.numeric_scale data_scale',
          db.case()
              .when(Op.eq('t.is_nullable', 'YES'))
              .then(0)
              .else(1)
              .as('not_null'),
          't.domain_schema', 't.domain_name',
          db.select(
              db.raw('(select pg_catalog.col_description(c.oid, t.ordinal_position))')
          ).from('pg_catalog.pg_class c')
              .where(Op.eq('c.relname', db.raw('t.table_name')))
              .as('column_comments')
      )
      .from('information_schema.columns t')
      .orderBy('t.table_schema', 't.table_name', 't.ordinal_position');
};

proto.queryPrimaryKeys = function(db) {
  const Op = db.Op;
  return db
      .select('t.constraint_schema schema_name', 't.table_name', 't.constraint_name',
          db.raw('string_agg(u.column_name, \',\') column_names')
      )
      .from('information_schema.table_constraints t')
      .join(
          db.join('information_schema.constraint_column_usage u')
              .on(Op.eq('u.constraint_catalog', db.raw('t.constraint_catalog')),
                  Op.eq('u.constraint_schema', db.raw('t.constraint_schema')),
                  Op.eq('u.constraint_name', db.raw('t.constraint_name'))
              )
      ).where(Op.eq('t.constraint_type', 'PRIMARY KEY'))
      .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name');
};

proto.queryForeignKeys = function(db) {
  const Op = db.Op;
  const q = db
      .select('t.constraint_schema schema_name', 't.table_name', 't.constraint_name',
          'kcu.column_name',
          'ccu.constraint_catalog foreign_catalog',
          'ccu.table_schema foreign_schema',
          'ccu.table_name foreign_table_name',
          db.raw('string_agg(ccu.column_name, \',\') foreign_column_name')
      )
      .from('information_schema.table_constraints t')
      .join(
          db.join('information_schema.key_column_usage kcu')
              .on(Op.eq('kcu.constraint_catalog', db.raw('t.constraint_catalog')),
                  Op.eq('kcu.constraint_schema', db.raw('t.constraint_schema')),
                  Op.eq('kcu.constraint_name', db.raw('t.constraint_name'))
              ),
          db.join('information_schema.constraint_column_usage ccu')
              .on(Op.eq('ccu.constraint_catalog', db.raw('t.constraint_catalog')),
                  Op.eq('ccu.constraint_schema', db.raw('t.constraint_schema')),
                  Op.eq('ccu.constraint_name', db.raw('t.constraint_name'))
              )
      ).where(Op.eq('t.constraint_type', 'FOREIGN KEY'))
      .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name',
          't.table_name', 'kcu.column_name', 'ccu.constraint_catalog',
          'ccu.table_schema', 'ccu.table_name');
  console.log(q.generate().sql);
  return q;
};
