/* sqb-connect-oracle
 ------------------------
 (c) 2017-present Panates
 SQB may be freely distributed under the MIT license.
 For details and documentation:
 https://panates.github.io/sqb-connect-oracle/
 */

/**
 * Expose `PgMetaOperator`.
 */
module.exports = PgMetaOperator;

/**
 * @constructor
 */
function PgMetaOperator() {
}

const proto = PgMetaOperator.prototype = {};
proto.constructor = PgMetaOperator;

/**
 * @param {String} tableName
 * @return {String}
 * @protected
 */
proto.getSelectSql = function(tableName) {
  switch (tableName) {
    case 'schemas':
      return 'select username schema_name, created create_date from all_users';
    case 'tables':
      return 'select owner schema_name, table_name, num_rows, \n' +
          '  (select comments from all_tab_comments atc \n' +
          '    where atc.owner = tbl.owner and atc.table_name = tbl.table_name) table_comments\n' +
          'from all_tables tbl\n' +
          'order by tbl.owner, tbl.table_name';
    case 'columns':
      return 'select owner schema_name, table_name, column_name,\n' +
          '  case when substr(data_type,0,9)=\'TIMESTAMP\' then \'TIMESTAMP\' else ' +
          'decode(data_type,' +
          '\'NCHAR\',\'CHAR\',' +
          '\'NCLOB\',\'NCLOB\',' +
          '\'VARCHAR2\',\'VARCHAR\',' +
          '\'NVARCHAR2\',\'VARCHAR\',' +
          '\'LONG\',\'VARCHAR\',' +
          '\'ROWID\',\'VARCHAR\',' +
          '\'UROWID\',\'VARCHAR\',' +
          '\'LONG RAW\',\'BUFFER\',' +
          '\'BINARY_FLOAT\',\'BUFFER\',' +
          '\'BINARY_DOUBLE\',\'BUFFER\',' +
          '\'RAW\', \'BUFFER\',' +
          'data_type) end data_type,\n' +
          '  data_length, data_precision, data_scale, decode(nullable, \'Y\', 1, 0) nullable,\n' +
          '  (select comments from all_col_comments acc where ' +
          'acc.owner = atc.owner and ' +
          'acc.table_name = atc.table_name and ' +
          'acc.column_name = atc.column_name) column_comments\n' +
          'from all_tab_columns atc\n' +
          'order by atc.owner, atc.table_name, atc.column_id';
    case 'primary_keys':
      return 'select ac.owner schema_name, ac.table_name, ac.constraint_name, \n' +
          '  to_char(listagg(acc.column_name, \',\') within group (order by null)) columns, \n' +
          '  decode(ac.status, \'ENABLED\', 1, 0) enabled\n' +
          'from all_constraints ac\n' +
          'inner join all_cons_columns acc on acc.owner = ac.owner and' +
          ' acc.table_name = ac.table_name and acc.constraint_name = ac.constraint_name\n' +
          'where ac.constraint_type = \'P\'\n' +
          'group by ac.owner, ac.table_name, ac.constraint_name, ac.status\n' +
          'order by ac.owner, ac.table_name, ac.constraint_name';
    case 'foreign_keys':
      return 'select ac.owner schema_name, ac.table_name, ac.constraint_name,\n' +
          '  to_char(listagg(acc.column_name, \',\') within group (order by null)) column_name,\n' +
          '  to_char(listagg(ac.r_owner, \',\') within group (order by null)) r_schema,\n' +
          '  to_char(listagg(acr.table_name, \',\') within group (order by null)) r_table_name,\n' +
          '  to_char(listagg(acr.column_name, \',\') within group (order by null)) r_columns,\n' +
          '  decode(ac.status, \'ENABLED\', 1, 0) enabled\n' +
          'from all_constraints ac\n' +
          'inner join all_cons_columns acc on acc.owner = ac.owner and' +
          ' acc.constraint_name = ac.constraint_name\n' +
          'inner join all_cons_columns acr on acr.owner = ac.r_owner and' +
          ' acr.constraint_name = ac.r_constraint_name\n' +
          'where ac.constraint_type = \'R\'\n' +
          'group by ac.owner, ac.table_name, ac.constraint_name, ac.status\n' +
          'order by ac.owner, ac.table_name, ac.constraint_name';
  }
  throw new Error('Unknown meta-data table `' + tableName + '`');
};
