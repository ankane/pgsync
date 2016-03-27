$VERBOSE=false

require "bundler/setup"
Bundler.require(:default)
require "minitest/autorun"
require "minitest/pride"

%x[psql -d pgsync_from -f test/support/from.sql]
%x[psql -d pgsync_to -f test/support/to.sql]
