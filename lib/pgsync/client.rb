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
    rescue Error, PG::ConnectionBad, Slop::Error => e
      abort colorize(e.message, :red)
    end

    def self.start
      new(ARGV).perform
    end

    protected

    def slop_options
      o = Slop::Options.new
      o.banner = %{Usage:
  pgsync [options]

Options:}
      o.string "-d", "--db", "database"
      o.string "-t", "--tables", "tables to sync"
      o.string "-g", "--groups", "groups to sync"
      o.integer "-j", "--jobs", "number of tables to sync at a time"
      o.string "--schemas", "schemas to sync"
      o.string "--from", "source"
      o.string "--to", "destination"
      o.string "--exclude", "exclude tables"
      o.string "--config", "config file"
      o.boolean "--to-safe", "accept danger", default: false
      o.boolean "--debug", "debug", default: false
      o.boolean "--list", "list", default: false
      o.boolean "--overwrite", "overwrite existing rows", default: false, help: false
      o.boolean "--preserve", "preserve existing rows", default: false
      o.boolean "--truncate", "truncate existing rows", default: false
      o.boolean "--schema-first", "schema first", default: false
      o.boolean "--schema-only", "schema only", default: false
      o.boolean "--all-schemas", "all schemas", default: false
      o.boolean "--no-rules", "do not apply data rules", default: false
      o.boolean "--no-sequences", "do not sync sequences", default: false
      o.boolean "--init", "init", default: false
      o.boolean "--in-batches", "in batches", default: false, help: false
      o.integer "--batch-size", "batch size", default: 10000, help: false
      o.float "--sleep", "sleep", default: 0, help: false
      o.boolean "--fail-fast", "stop on the first failed table", default: false
      o.boolean "--defer-constraints", "defer constraints", default: false
      o.boolean "--disable-user-triggers", "disable non-system triggers", default: false
      o.boolean "--disable-integrity", "disable foreign key triggers", default: false
      o.boolean "-v", "--version", "print the version"
      o.boolean "-h", "--help", "prints help"
      o
    end
  end
end
