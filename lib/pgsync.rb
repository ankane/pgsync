# dependencies
require "parallel"
require "pg"
require "slop"
require "tty-spinner"

# stdlib
require "cgi"
require "erb"
require "fileutils"
require "set"
require "shellwords"
require "tempfile"
require "thread" # windows only
require "uri"
require "yaml"

# modules
require "pgsync/utils"
require "pgsync/client"
require "pgsync/data_source"
require "pgsync/init"
require "pgsync/sync"
require "pgsync/table_list"
require "pgsync/table_sync"
require "pgsync/version"

module PgSync
  class Error < StandardError; end
end
