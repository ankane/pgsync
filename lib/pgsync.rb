# dependencies
require "parallel"
require "pg"
require "slop"
require "tty-spinner"

# stdlib
require "open3"
require "set"
require "shellwords"
require "tempfile"
require "uri"
require "yaml"

# modules
require_relative "pgsync/utils"
require_relative "pgsync/client"
require_relative "pgsync/data_source"
require_relative "pgsync/init"
require_relative "pgsync/schema_sync"
require_relative "pgsync/sequence"
require_relative "pgsync/sync"
require_relative "pgsync/table"
require_relative "pgsync/table_sync"
require_relative "pgsync/task"
require_relative "pgsync/task_resolver"
require_relative "pgsync/version"

module PgSync
  class Error < StandardError; end
end
