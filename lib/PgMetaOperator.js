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

const proto = PgMetaOperator.prototype = {};
proto.constructor = PgMetaOperator;

proto.query = function(sqbObj, request, callback) {
  const response = {
    schemas: {}
  };

  waterfall([
    function(next) {
      fetchSchemas(sqbObj, request, response, next);
    },
    function(next) {
      fetchTables(sqbObj, request, response, next);
    }
  ], function(err) {
    if (err)
      return callback(err);
    callback(undefined, response);
  });

};

function fetchSchemas(sqbObj, request, response, callback) {
  var schemaKeys;
  if (request.schemas !== '*') {
    // Replace schema names to upper case names
    schemaKeys = Object.getOwnPropertyNames(request.schemas)
        .map(function(key) {
          const upname = String(key).toUpperCase();
          if (upname !== key) {
            request.schemas[upname] = request.schemas[key];
            delete request.schemas[key];
          }
          return upname;
        });
  }

  const query = sqbObj
      .select('schema_name')
      .from('information_schema.schemata')
      .where(['schema_name', '!=', ['information_schema', 'pg_catalog']],
          ['schema_name', '!like', 'pg_toast%'],
          ['schema_name', '!like', 'pg_temp_%']);
  if (schemaKeys && schemaKeys.length)
    query.where(['schema_name', schemaKeys]);
  query.execute(function(err, result) {
    if (err)
      return callback(err);
    const rowset = result.rowset;
    var s;
    while (rowset.next()) {
      s = rowset.get('schema_name');
      response.schemas[s] = {};
    }
    callback();
  });
}

function fetchTables(sqbObj, request, response, callback) {
  const IgnoreBuildInSchemas = [
    ['table_schema', '!=', ['information_schema', 'pg_catalog']],
    ['table_schema', '!like', 'pg_toast%'],
    ['table_schema', '!like', 'pg_temp_%']];

  const schemaKeys = Object.getOwnPropertyNames(response.schemas);
  const where = [];
  if (schemaKeys && schemaKeys.length) {
    schemaKeys.forEach(function(t) {
      const g = [['t.table_schema', t]];
      const sch = typeof request.schemas === 'object' && request.schemas[t];
      if (sch && sch !== '*' && sch.tables && sch.tables !== '*') {
        g.push(['t.table_name', sch.tables]);
      }
      if (where.length)
        where.push('or');
      where.push(g);
    });
  }

  waterfall([
    // Fetch tables
    function(next) {
      const query = sqbObj
          .select('t.table_schema', 't.table_name', 't.commit_action',
              sqbObj.select(
                  sqbObj.raw('(select pg_catalog.obj_description(c.oid))')
              ).from('pg_catalog.pg_class c')
                  .where(['c.relname', sqbObj.raw('t.table_name')])
                  .as('table_comments')
          )
          .from('information_schema.tables t')
          .where(IgnoreBuildInSchemas, ['table_type', 'BASE TABLE'])
          .orderBy('t.table_schema', 't.table_name');
      if (where.length)
        query.where(where);

      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000
      }, function(err, result) {
        if (err)
          return callback(err);
        const cursor = result.cursor;
        var schema;
        var o;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.table_schema];
          schema.tables = schema.tables || {};
          o = {
            comments: row.table_comments
          };
          if (row.commit_action === 'PRESERVE')
            o.temporary = true;
          o.columns = {};
          schema.tables[row.table_name] = o;
          more();
        });
      });
    },

    // Fetch columns
    function(next) {
      const query = sqbObj
          .select('table_schema', 'table_name', 't.column_name', 't.ordinal_position', 't.is_nullable',
              't.data_type', 't.udt_name', 't.character_maximum_length',
              't.numeric_precision', 't.numeric_precision_radix', 't.numeric_scale',
              't.domain_schema', 't.domain_name',
              sqbObj.select(
                  sqbObj.raw('(select pg_catalog.col_description(c.oid, t.ordinal_position))')
              ).from('pg_catalog.pg_class c')
                  .where(['c.relname', sqbObj.raw('t.table_name')])
                  .as('column_comments')
          )
          .from('information_schema.columns t')
          .where(IgnoreBuildInSchemas)
          .orderBy('t.table_schema', 't.table_name', 't.ordinal_position');
      if (where.length)
        query.where.apply(query, where);
      //console.log(query.generate().sql);
      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        var o;
        var v;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.table_schema];
          table = schema && schema.tables[row.table_name];
          if (table) {
            o = {
              position: row.ordinal_position,
              dataType: row.udt_name.toUpperCase(),
              data_type_org: row.data_type,
              udt_name: row.udt_name
            };
            switch (row.udt_name) {
              case 'int2':
                o.dataType = 'SMALLINT';
                break;
              case 'int4':
                o.dataType = 'INTEGER';
                break;
              case 'int8':
                o.dataType = 'BIGINT';
                break;
              case 'float4':
                o.dataType = 'FLOAT';
                break;
              case 'float8':
                o.dataType = 'DOUBLE';
                break;
              case 'numeric':
                o.dataType = 'NUMBER';
                break;
              case 'bpchar':
              case 'text':
                o.dataType = 'VARCHAR';
                break;
              case 'bytea':
                o.dataType = 'BUFFER';
                break;
            }
            if ((v = row.character_maximum_length))
              o.char_len = v;
            if ((v = row.numeric_precision)) {
              o.precision = v;
              if ((v = row.numeric_precision_radix))
                o.precision_radix = v;
            }
            if ((v = row.numeric_scale))
              o.scale = v;
            if ((v = row.domain_schema))
              o.domain_schema = v;
            if ((v = row.domain_name))
              o.domain_name = v;

            if (row.NULLABLE !== 'Y')
              o.notnull = true;
            if ((v = row.column_comments))
              o.comments = v;
            table.columns[row.column_name] = o;
          }
          more();
        });
      });
    },

    // Fetch primary keys
    function(next) {
      const query = sqbObj
          .select('t.constraint_schema', 't.table_name', 't.constraint_name',
              sqbObj.raw('string_agg(u.column_name, \',\') column_names')
          )
          .from('information_schema.table_constraints t')
          .join(
              sqbObj.join('information_schema.constraint_column_usage u')
                  .on(['u.constraint_catalog',
                        sqbObj.raw('t.constraint_catalog')],
                      ['u.constraint_schema',
                        sqbObj.raw('t.constraint_schema')],
                      ['u.constraint_name', sqbObj.raw('t.constraint_name')]
                  )
          ).where(['t.constraint_type', 'PRIMARY KEY'])
          .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name');
      if (where.length)
        query.where(where);
      //console.log(query.generate().sql);
      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.constraint_schema];
          table = schema && schema.tables[row.table_name];
          if (table) {
            table.primaryKey = {
              constraint_name: row.constraint_name,
              columns: row.column_names
            };
          }
          more();
        });
      });
    },

    // Foreign keys
    function(next) {
      const query = sqbObj
          .select('t.constraint_schema', 't.table_name', 't.constraint_name',
              'kcu.column_name',
              'ccu.constraint_catalog foreign_catalog',
              'ccu.constraint_schema foreign_schema',
              'ccu.table_name foreign_table_name',
              sqbObj.raw('string_agg(ccu.column_name, \',\') foreign_columns')
          )
          .from('information_schema.table_constraints t')
          .join(
              sqbObj.join('information_schema.key_column_usage kcu')
                  .on(['kcu.constraint_catalog',
                        sqbObj.raw('t.constraint_catalog')],
                      ['kcu.constraint_schema',
                        sqbObj.raw('t.constraint_schema')],
                      ['kcu.constraint_name', sqbObj.raw('t.constraint_name')]
                  ),
              sqbObj.join('information_schema.constraint_column_usage ccu')
                  .on(['ccu.constraint_catalog',
                        sqbObj.raw('t.constraint_catalog')],
                      ['ccu.constraint_schema',
                        sqbObj.raw('t.constraint_schema')],
                      ['ccu.constraint_name', sqbObj.raw('t.constraint_name')]
                  )
          ).where(['t.constraint_type', 'FOREIGN KEY'])
          .groupBy('t.constraint_schema', 't.table_name', 't.constraint_name',
              't.table_name', 'kcu.column_name', 'ccu.constraint_catalog',
              'ccu.constraint_schema', 'ccu.table_name');
      if (where.length)
        query.where(where);
      //console.log(query.generate().sql);
      query.execute({
        cursor: true,
        objectRows: true,
        fetchRows: 1000,
        naming: 'uppercase'
      }, function(err, result) {
        if (err)
          return callback(err);
        var schema;
        var table;
        const cursor = result.cursor;
        cursor.next(function(err, row, more) {
          if (err || !more) {
            cursor.close(function(err2) {
              next(err || err2);
            });
            return;
          }
          schema = response.schemas[row.constraint_schema];
          table = schema && schema.tables[row.table_name];
          if (table) {
            table.primaryKey = {
              constraint_name: row.constraint_name,
              column_name: row.column_name,
              foreign_catalog: row.foreign_catalog,
              foreign_schema: row.foreign_schema,
              foreign_table_name: row.foreign_table_name,
              foreign_columns: row.foreign_columns
            };
          }
          more();
        });
      });
    }
  ], callback);
}
