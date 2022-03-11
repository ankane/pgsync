module PgSync
  class Client
    include Utils

    def initialize(args)
      @args = args
      output.sync = true
    end

    def perform
      result = Slop::Parser.new(slop_options).parse(@args)
      arguments = result.arguments
      options = result.to_h
      options[:defer_constraints_v2] ||= options[:defer_constraints]

      raise Error, "Specify either --db or --config, not both" if options[:db] && options[:config]
      raise Error, "Cannot use --overwrite with --in-batches" if options[:overwrite] && options[:in_batches]

      if options[:version]
        log VERSION
      elsif options[:help]
        log slop_options
      elsif options[:init]
        Init.new(arguments, options).perform
      else
        Sync.new(arguments, options).perform
      end
    rescue => e
      # Error, PG::ConnectionBad, Slop::Error
      raise e if options && options[:debug]
      abort colorize(e.message.strip, :red)
    end

    def self.start
      new(ARGV).perform
    end

    protected

    def slop_options
      o = Slop::Options.new
      o.banner = %{Usage:
    pgsync [tables,groups] [sql] [options]}

      # not shown
      o.string "-t", "--tables", "tables to sync", help: false
      o.string "-g", "--groups", "groups to sync", help: false

      o.separator ""
      o.separator "Table options:"
      o.string "--exclude", "tables to exclude"
      o.string "--schemas", "schemas to sync"
      o.boolean "--all-schemas", "sync all schemas", default: false

      o.separator ""
      o.separator "Row options:"
      o.boolean "--overwrite", "overwrite existing rows", default: false
      o.boolean "--preserve", "preserve existing rows", default: false
      o.boolean "--truncate", "truncate existing rows", default: false

      o.separator ""
      o.separator "Foreign key options:"
      o.boolean "--defer-constraints", "defer constraints", default: false
      o.boolean "--disable-integrity", "disable foreign key triggers", default: false
      o.integer "-j", "--jobs", "number of tables to sync at a time"

      # legacy
      o.boolean "--defer-constraints-v1", "defer constraints", default: false, help: false
      o.boolean "--defer-constraints-v2", "defer constraints", default: false, help: false
      # private, for testing
      o.boolean "--disable-integrity-v2", "disable foreign key triggers", default: false, help: false

      o.separator ""
      o.separator "Schema options:"
      o.boolean "--schema-first", "sync schema first", default: false
      o.boolean "--schema-only", "sync schema only", default: false

      o.separator ""
      o.separator "Config options:"
      # technically, defaults to searching path for .pgsync.yml, but this is simpler
      o.string "--config", "config file (defaults to .pgsync.yml)"
      o.string "-d", "--db", "database-specific config file"

      o.separator ""
      o.separator "Connection options:"
      o.string "--from", "source database URL"
      o.string "--to", "destination database URL"
      o.boolean "--to-safe", "confirms destination is safe (when not localhost)", default: false

      o.separator ""
      o.separator "Other options:"
      o.boolean "--debug", "show SQL statements", default: false
      o.boolean "--disable-user-triggers", "disable non-system triggers", default: false
      o.boolean "--fail-fast", "stop on the first failed table", default: false
      o.boolean "--no-rules", "don't apply data rules", default: false
      o.boolean "--no-sequences", "don't sync sequences", default: false

      # not shown in help
      # o.separator ""
      # o.separator "Append-only table options:"
      o.boolean "--in-batches", "sync in batches", default: false, help: false
      o.integer "--batch-size", "batch size", default: 10000, help: false
      o.float "--sleep", "time to sleep between batches", default: 0, help: false

      o.separator ""
      o.separator "Other commands:"
      o.boolean "--init", "create config file", default: false
      o.boolean "--list", "list tables", default: false
      o.boolean "-h", "--help", "print help"
      o.boolean "-v", "--version", "print version"

      o
    end
  end
end
