module PgSync
  class Client
    def initialize(args)
      $stdout.sync = true
      @exit = false
      @arguments, @options = parse_args(args)
      @mutex = windows? ? Mutex.new : MultiProcessing::Mutex.new
    end

    # TODO clean up this mess
    def perform
      return if @exit

      args, opts = @arguments, @options
      [:to, :from, :to_safe, :exclude].each do |opt|
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

      unless opts[:to_safe] || destination.local?
        raise PgSync::Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
      end

      print_description("From", source)
      print_description("To", destination)

      tables = nil
      begin
        tables = TableList.new(args, opts, source, config).tables
      ensure
        source.close
      end

      unless opts[:all_schemas]
        tables.select! { |t| t.start_with?("#{source.schema}.") }
      end

      confirm_tables_exist(source, tables, "source")

      if opts[:list]
        confirm_tables_exist(destination, tables, "destination")

        if args[0] == "groups"
          pretty_list (config["groups"] || {}).keys
        else
          pretty_list tables.keys
        end
      else
        if opts[:schema] || opts[:schema_only]
          log "* Dumping schema"
          sync_schema(source, destination, tables)
        end

        unless opts[:schema_only]
          confirm_tables_exist(destination, tables, "destination")

          in_parallel(tables) do |table, table_opts|
            sync_table(table, opts.merge(table_opts), source.url, destination.url)
          end
        end

        log_completed(start_time)
      end
    end

    def confirm_tables_exist(destination, tables, description)
      tables.keys.each do |table|
        unless destination.table_exists?(table)
          raise PgSync::Error, "Table does not exist in #{description}: #{table}"
        end
      end
    ensure
      destination.close
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

    def sync_table(table, opts, source_url, destination_url)
      time =
        benchmark do
          TableSync.new.sync(@mutex, config, table, opts, source_url, destination_url)
        end

      @mutex.synchronize do
        log "* DONE #{table} (#{time.round(1)}s)"
      end
    end

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = %{Usage:
    pgsync [options]

Options:}
        o.string "-t", "--tables", "tables"
        o.string "-g", "--groups", "groups"
        o.string "-d", "--db", "database"
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
        o.boolean "--schema-only", "schema only", default: false
        o.boolean "--schema", "with schema", default: false
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

    def benchmark
      start_time = Time.now
      yield
      Time.now - start_time
    end

    def quote_ident(value)
      PG::Connection.quote_ident(value)
    end

    def escape(value)
      if value.is_a?(String)
        "'#{quote_string(value)}'"
      else
        value
      end
    end

    # activerecord
    def quote_string(s)
      s.gsub(/\\/, '\&\&').gsub(/'/, "''")
    end

    def print_description(prefix, source)
      log "#{prefix}: #{source.uri.path.sub(/\A\//, '')} on #{source.uri.host}:#{source.uri.port}"
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
