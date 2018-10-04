/* eslint-disable */
const fs = require('fs');
const path = require('path');
process.env.NODE_ENV = 'test';

const f = path.join(__dirname, 'db_config.json');
if (fs.existsSync(f)) {
  const config = require(f);
  process.env.DB_USER = process.env.DB_USER || config.user;
  process.env.DB_PASS = process.env.DB_PASS || config.password;
  process.env.DB_HOST = process.env.DB_HOST || config.host;
  process.env.DB = process.env.DB || config.database;
  if (process.env.SKIP_CREATE_TABLES || config.skip_create_tables)
    process.env.SKIP_CREATE_TABLES = true;
}