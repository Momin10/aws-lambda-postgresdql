const PgConnection = require('postgresql-easy');
const dbConfig = require('./db');
const pg = new PgConnection(dbConfig);

module.exports = pg;