# dependencies
require "parallel"
require "pg"
require "slop"
require "tty-spinner"

# stdlib
require "set"
require "shellwords"
require "tempfile"
require "uri"
require "yaml"

# modules
require "pgsync/utils"
require "pgsync/client"
require "pgsync/data_source"
require "pgsync/init"
require "pgsync/schema_sync"
require "pgsync/sync"
require "pgsync/table_list"
require "pgsync/table_sync"
require "pgsync/version"

module PgSync
  class Error < StandardError; end
end
