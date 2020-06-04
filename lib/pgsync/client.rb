module PgSync
  class Client
    include Utils

    def initialize(args)
      @args = args
      output.sync = true
    end

    def perform(testing: true)
      opts = parse_args

      # TODO throw error in 0.6.0
      warn "Specify either --db or --config, not both" if opts[:db] && opts[:config]

      if opts.version?
        log VERSION
      elsif opts.help?
        log opts
      # TODO remove deprecated conditions (last two)
      elsif opts.init? || opts.setup? || opts.arguments[0] == "setup"
        Init.new.perform(opts)
      else
        Sync.new.perform(opts)
      end
    rescue Error, PG::ConnectionBad => e
      raise e if testing
      abort colorize(e.message, :red)
    end

    def self.start
      new(ARGV).perform(testing: false)
    end

    protected

    def parse_args
      Slop.parse(@args) do |o|
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
        o.string "--where", "where", help: false
        o.integer "--limit", "limit", help: false
        o.string "--exclude", "exclude tables"
        o.string "--config", "config file"
        # TODO much better name for this option
        o.boolean "--to-safe", "accept danger", default: false
        o.boolean "--debug", "debug", default: false
        o.boolean "--list", "list", default: false
        o.boolean "--overwrite", "overwrite existing rows", default: false, help: false
        o.boolean "--preserve", "preserve existing rows", default: false
        o.boolean "--truncate", "truncate existing rows", default: false
        o.boolean "--update", "update existing rows", default: false
        o.boolean "--schema-first", "schema first", default: false
        o.boolean "--schema-only", "schema only", default: false
        o.boolean "--all-schemas", "all schemas", default: false
        o.boolean "--no-rules", "do not apply data rules", default: false
        o.boolean "--no-sequences", "do not sync sequences", default: false
        o.boolean "--init", "init", default: false
        o.boolean "--setup", "setup", default: false, help: false
        o.boolean "--in-batches", "in batches", default: false, help: false
        o.integer "--batch-size", "batch size", default: 10000, help: false
        o.float "--sleep", "sleep", default: 0, help: false
        o.boolean "--fail-fast", "stop on the first failed table", default: false
        o.boolean "--defer-constraints", "defer constraints", default: false
        o.boolean "--disable-user-triggers", "disable non-system triggers", default: false
        o.boolean "--disable-integrity", "disable foreign key triggers", default: false
        # o.array "--var", "pass a variable"
        o.boolean "-v", "--version", "print the version"
        o.boolean "-h", "--help", "prints help"
      end
    rescue Slop::Error => e
      raise Error, e.message
    end
  end
end
