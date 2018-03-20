module PgSync
  class Client
    def initialize(args)
      $stdout.sync = true
      $stderr.sync = true
      @exit = false
      @arguments, @options = parse_args(args)
      @mutex = windows? ? Mutex.new : MultiProcessing::Mutex.new
    end

    # TODO clean up this mess
    def perform
      return if @exit

      args, opts = @arguments, @options
      [:to, :from, :to_safe, :exclude, :schemas].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end
      map_deprecations(args, opts)

      if opts[:setup]
        setup(db_config_file(args[0]) || config_file || ".pgsync.yml")
      else
        sync(args, opts)
      end

      true
    end

    protected

    def sync(args, opts)
      start_time = Time.now

      if args.size > 2
        raise PgSync::Error, "Usage:\n    pgsync [options]"
      end

      source = DataSource.new(opts[:from])
      raise PgSync::Error, "No source" unless source.exists?

      destination = DataSource.new(opts[:to])
      raise PgSync::Error, "No destination" unless destination.exists?

      begin
        # start connections
        source.host
        destination.host

        unless opts[:to_safe] || destination.local?
          raise PgSync::Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
        end

        print_description("From", source)
        print_description("To", destination)
      ensure
        source.close
        destination.close
      end

      tables = nil
      begin
        tables = TableList.new(args, opts, source, config).tables
      ensure
        source.close
      end

      confirm_tables_exist(source, tables, "source")

      if opts[:list]
        confirm_tables_exist(destination, tables, "destination")

        list_items =
          if args[0] == "groups"
            (config["groups"] || {}).keys
          else
            tables.keys
          end

        pretty_list list_items
      else
        if opts[:schema_first] || opts[:schema_only]
          if opts[:preserve]
            raise PgSync::Error, "Cannot use --preserve with --schema-first or --schema-only"
          end

          log "* Dumping schema"
          sync_schema(source, destination, tables)
        end

        if opts[:stats_only]
          log "* Dumping stats"
          sync_stats(source, destination, tables.keys)
        end

        if !opts[:schema_only] && !opts[:stats_only]
          confirm_tables_exist(destination, tables, "destination")

          in_parallel(tables) do |table, table_opts|
            TableSync.new.sync(@mutex, config, table, opts.merge(table_opts), source.url, destination.url, source.search_path.find { |sp| sp != "pg_catalog" })
          end
        end

        log_completed(start_time)
      end
    end

    def confirm_tables_exist(data_source, tables, description)
      tables.keys.each do |table|
        unless data_source.table_exists?(table)
          raise PgSync::Error, "Table does not exist in #{description}: #{table}"
        end
      end
    ensure
      data_source.close
    end

    def map_deprecations(args, opts)
      command = args[0]

      case command
      when "setup"
        args.shift
        opts[:setup] = true
        deprecated "Use `psync --setup` instead"
      when "schema"
        args.shift
        opts[:schema_only] = true
        deprecated "Use `psync --schema-only` instead"
      when "tables"
        args.shift
        opts[:tables] = args.shift
        deprecated "Use `pgsync #{opts[:tables]}` instead"
      when "groups"
        args.shift
        opts[:groups] = args.shift
        deprecated "Use `pgsync #{opts[:groups]}` instead"
      end

      if opts[:where]
        opts[:sql] ||= String.new
        opts[:sql] << " WHERE #{opts[:where]}"
        deprecated "Use `\"WHERE #{opts[:where]}\"` instead"
      end

      if opts[:limit]
        opts[:sql] ||= String.new
        opts[:sql] << " LIMIT #{opts[:limit]}"
        deprecated "Use `\"LIMIT #{opts[:limit]}\"` instead"
      end
    end

    def sync_schema(source, destination, tables)
      dump_command = source.dump_command(tables)
      restore_command = destination.restore_command
      system("#{dump_command} | #{restore_command}")
    end

    def sync_stats(source, destination, tables)
      destination_oids = Hash[destination.table_stats(tables).map { |ts| [ts["table"], ts["oid"]] }]
      table_stats = Hash[source.table_stats(tables).map { |ts| [ts["table"], ts] }]

      # create mapping
      oid_mapping = {}
      table_stats.each do |_, ts|
        oid_mapping[ts["oid"]] = destination_oids[ts["table"]]
      end

      i = 1
      queries = []
      params = []
      source.column_stats(oid_mapping.keys).each do |row|
        query = []
        row.each do |k, v|
          if k == "starelid"
            v = oid_mapping[v]
          end

          if k.start_with?("stavalues")
            query << "array_in($#{i}, 25, -1)"
          else
            query << "$#{i}"
          end
          params << v
          i += 1
        end
        queries << query
      end
      sql = queries.map { |r| "(#{r.join(", ")})" }.join(",")

      conn = destination.conn
      conn.transaction do
        # update pg_catalog
        tables.each do |table|
          ts = table_stats[table]
          conn.exec_params("UPDATE pg_catalog.pg_class SET relpages = $1, reltuples = $2, relallvisible = $3 WHERE oid = $4", [ts["relpages"], ts["reltuples"], ts["relallvisible"], destination_oids[table]])
        end

        # update pg_statistic
        conn.exec_params("DELETE FROM pg_catalog.pg_statistic WHERE starelid IN (#{destination_oids.values.join(", ")})", [])
        conn.exec_params("INSERT INTO pg_catalog.pg_statistic VALUES #{sql}", params)
      end
    ensure
      source.close
      destination.close
    end

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = %{Usage:
    pgsync [options]

Options:}
        o.string "-d", "--db", "database"
        o.string "-t", "--tables", "tables to sync"
        o.string "-g", "--groups", "groups to sync"
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
        o.boolean "--schema-first", "schema first", default: false
        o.boolean "--schema-only", "schema only", default: false
        o.boolean "--stats-only", "stats only", default: false
        o.boolean "--all-schemas", "all schemas", default: false
        o.boolean "--no-rules", "do not apply data rules", default: false
        o.boolean "--setup", "setup", default: false
        o.boolean "--in-batches", "in batches", default: false, help: false
        o.integer "--batch-size", "batch size", default: 10000, help: false
        o.float "--sleep", "sleep", default: 0, help: false
        o.on "-v", "--version", "print the version" do
          log PgSync::VERSION
          @exit = true
        end
        o.on "-h", "--help", "prints help" do
          log o
          @exit = true
        end
      end
      [opts.arguments, opts.to_hash]
    rescue Slop::Error => e
      raise PgSync::Error, e.message
    end

    def config
      @config ||= begin
        if config_file
          begin
            YAML.load_file(config_file) || {}
          rescue Psych::SyntaxError => e
            raise PgSync::Error, e.message
          end
        else
          {}
        end
      end
    end

    def setup(config_file)
      if File.exist?(config_file)
        raise PgSync::Error, "#{config_file} exists."
      else
        FileUtils.cp(File.dirname(__FILE__) + "/../../config.yml", config_file)
        log "#{config_file} created. Add your database credentials."
      end
    end

    def db_config_file(db)
      return unless db
      ".pgsync-#{db}.yml"
    end

    def print_description(prefix, source)
      location = " on #{source.host}:#{source.port}" if source.host
      log "#{prefix}: #{source.dbname}#{location}"
    end

    def search_tree(file)
      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        if File.exist?(absolute_file)
          break absolute_file
        end
        path = File.dirname(path)
        break if path == "/"
      end
    end

    def config_file
      return @config_file if instance_variable_defined?(:@config_file)

      @config_file =
        search_tree(
          if @options[:db]
            db_config_file(@options[:db])
          else
            @options[:config] || ".pgsync.yml"
          end
        )
    end

    def log(message = nil)
      $stderr.puts message
    end

    def in_parallel(tables, &block)
      if @options[:debug] || @options[:in_batches]
        tables.each(&block)
      else
        options = {}
        options[:in_threads] = 4 if windows?
        Parallel.each(tables, options, &block)
      end
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
    end

    def deprecated(message)
      log "[DEPRECATED] #{message}"
    end

    def log_completed(start_time)
      time = Time.now - start_time
      log "Completed in #{time.round(1)}s"
    end

    def windows?
      Gem.win_platform?
    end
  end
end
