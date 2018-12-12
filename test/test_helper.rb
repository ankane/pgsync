require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

%x[
psql -d pgsync_test1 -f test/support/db1.sql
psql -d pgsync_test2 -f test/support/db2.sql
]
