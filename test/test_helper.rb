require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

%x[createdb pgsync_db1]
%x[createdb pgsync_db2]
%x[psql -d pgsync_db1 -f test/support/db1.sql]
%x[psql -d pgsync_db2 -f test/support/db2.sql]
