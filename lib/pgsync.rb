require "yaml"
require "slop"
require "uri"
require "erb"
require "pg"
require "parallel"
require "multiprocessing"
require "fileutils"
require "tempfile"
require "cgi"
require "shellwords"
require "thread" # windows only

require "pgsync/client"
require "pgsync/data_source"
require "pgsync/table_list"
require "pgsync/table_sync"
require "pgsync/version"

module URI
  class POSTGRESQL < Generic
    DEFAULT_PORT = 5432
  end
  @@schemes["POSTGRESQL"] = @@schemes["POSTGRES"] = POSTGRESQL
end

module PgSync
  class Error < StandardError; end
end
