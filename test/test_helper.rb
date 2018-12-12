require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"
require "pg"

conn1 = PG::Connection.open(dbname: "pgsync_test1")
conn1.exec <<-SQL
DROP TABLE IF EXISTS "Users";
CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY,
  email TEXT,
  token TEXT,
  zip_code TEXT
);
SQL
conn1.close

conn2 = PG::Connection.open(dbname: "pgsync_test2")
conn2.exec <<-SQL
DROP TABLE IF EXISTS "Users";
CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY,
  email TEXT,
  token TEXT
);
SQL
conn2.close
