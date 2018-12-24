/* sqb-connect-pg
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-pg/
 */

/**
 * @param {Object} sqbObj
 * @constructor
 */
class PgMetaOperator {

  constructor() {
    // noinspection JSUnusedGlobalSymbols
    this.supportsSchemas = true;
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  querySchemas(sqb) {
    return sqb
        .select('schema_name')
        .from('information_schema.schemata');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryTables(db) {
    return db
        .select('t.table_schema schema_name', 't.table_name', 't.commit_action',
            db.raw(`(select * from (select pg_catalog.obj_description(c.oid) from pg_catalog.pg_class c
     where c.relname = t.table_name and c.relkind = 'r') d
	 where d.obj_description is not null limit 1) table_comments`)
        )
        .from('information_schema.tables t')
        .where({'table_type': 'BASE TABLE'})
        .orderBy('t.table_schema', 't.table_name');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryColumns(db) {
    return db
        .select('t.table_schema schema_name', 't.table_name', 't.column_name',
            't.ordinal_position column_position', 't.column_default default_value',
            't.data_type',
            db.case()
                .when({udt_name: 'int2'}).then('SMALLINT')
                .when({udt_name: 'int4'}).then('INTEGER')
                .when({udt_name: 'int8'}).then('BIGINT')
                .when({udt_name: 'float4'}).then('FLOAT')
                .when({udt_name: 'float8'}).then('DOUBLE')
                .when({udt_name: 'numeric'}).then('NUMBER')
                .when({udt_name: 'bpchar'}).then('CHAR')
                .when({udt_name: 'text'}).then('VARCHAR')
                .when({udt_name: 'numeric'}).then('NUMBER')
                .else(db.raw('upper(t.udt_name)')).as('data_type_mean'),
            't.udt_name',
            't.character_maximum_length char_length',
            't.numeric_precision data_precision', 't.numeric_scale data_scale',
            db.case()
                .when({'t.is_nullable': 'YES'})
                .then(0)
                .else(1)
                .as('not_null'),
            't.domain_schema', 't.domain_name',
            db.raw(`(select * from (
  		select pg_catalog.col_description(c.oid, t.ordinal_position)
   		from pg_catalog.pg_class c
  		where c.relname = t.table_name and c.relkind = 'r') d
	 where d.col_description is not null limit 1) column_comments`)
        )
        .from('information_schema.columns t')
        .orderBy('t.table_schema', 't.table_name', 't.ordinal_position');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryPrimaryKeys(db) {
    return db
        .select('t.constraint_schema schema_name', 't.table_name', 't.constraint_name',
            db.raw('string_agg(u.column_name, \',\') column_names')
        )
        .from('information_schema.table_constraints t')
        .join(
            db.join('information_schema.constraint_column_usage u')
                .on({
                  'u.constraint_catalog': db.raw('t.constraint_catalog'),
                  'u.constraint_schema': db.raw('t.constraint_schema'),
                  'u.constraint_name': db.raw('t.constraint_name')
                })
        ).where({'t.constraint_type': 'PRIMARY KEY'})
        .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name');
  }

  // noinspection JSMethodCanBeStatic, JSUnusedGlobalSymbols
  queryForeignKeys(db) {
    return db
        .select(
            't.constraint_schema schema_name',
            't.table_name',
            't.constraint_name',
            'kcu.column_name',
            'ccu.constraint_catalog foreign_catalog',
            'ccu.table_schema foreign_schema',
            'ccu.table_name foreign_table_name',
            'ccu.column_name foreign_column_name'
        )
        .from('information_schema.table_constraints t')
        .join(
            db.leftOuterJoin(
                db.select('constraint_catalog', 'constraint_schema', 'constraint_name',
                    db.raw('string_agg(column_name, \',\') column_name')
                ).from('information_schema.key_column_usage')
                    .groupBy('1', '2', '3')
                    .as('kcu')
            ).on({
              'kcu.constraint_catalog': db.raw('t.constraint_catalog'),
              'kcu.constraint_schema': db.raw('t.constraint_schema'),
              'kcu.constraint_name': db.raw('t.constraint_name')
            }),

            db.leftOuterJoin(
                db.select('constraint_catalog', 'constraint_schema', 'constraint_name',
                    'table_schema', 'table_name',
                    db.raw('string_agg(column_name, \',\') column_name')
                ).from('information_schema.constraint_column_usage')
                    .groupBy('1', '2', '3', '4', '5')
                    .as('ccu')
            ).on({
              'ccu.constraint_catalog': db.raw('t.constraint_catalog'),
              'ccu.constraint_schema': db.raw('t.constraint_schema'),
              'ccu.constraint_name': db.raw('t.constraint_name')
            })
        ).where({'t.constraint_type': 'FOREIGN KEY'});
  }

}

/**
 * Expose `PgMetaOperator`.
 */
module.exports = PgMetaOperator;
