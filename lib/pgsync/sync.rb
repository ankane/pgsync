module PgSync
  class Sync
    include Utils

    def perform(options)
      args = options.arguments
      opts = options.to_hash
      @options = opts

      # merge config
      [:to, :from, :to_safe, :exclude, :schemas].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end

      # TODO remove deprecations
      map_deprecations(args, opts)

      # start
      start_time = Time.now

      if args.size > 2
        raise Error, "Usage:\n    pgsync [options]"
      end

      source = DataSource.new(opts[:from])
      raise Error, "No source" unless source.exists?

      destination = DataSource.new(opts[:to])
      raise Error, "No destination" unless destination.exists?

      begin
        # start connections
        source.host
        destination.host

        unless opts[:to_safe] || destination.local?
          raise Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
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
            raise Error, "Cannot use --preserve with --schema-first or --schema-only"
          end

          log "* Dumping schema"
          schema_tables = tables if !opts[:all_schemas] || opts[:tables] || opts[:groups] || args[0] || opts[:exclude]
          sync_schema(source, destination, schema_tables)
        end

        unless opts[:schema_only]
          confirm_tables_exist(destination, tables, "destination")

          in_parallel(tables, first_schema: source.search_path.find { |sp| sp != "pg_catalog" }) do |table, table_opts|
            TableSync.new.sync(config, table, opts.merge(table_opts), source.url, destination.url)
          end
        end

        log_completed(start_time)
      end
    end

    def confirm_tables_exist(data_source, tables, description)
      tables.keys.each do |table|
        unless data_source.table_exists?(table)
          raise Error, "Table does not exist in #{description}: #{table}"
        end
      end
    ensure
      data_source.close
    end

    def map_deprecations(args, opts)
      command = args[0]

      case command
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

    def sync_schema(source, destination, tables = nil)
      dump_command = source.dump_command(tables)
      restore_command = destination.restore_command
      unless system("#{dump_command} | #{restore_command}")
        raise Error, "Schema sync returned non-zero exit code"
      end
    end

    def config
      @config ||= begin
        file = config_file
        if file
          begin
            YAML.load_file(file) || {}
          rescue Psych::SyntaxError => e
            raise Error, e.message
          end
        else
          {}
        end
      end
    end

    def print_description(prefix, source)
      location = " on #{source.host}:#{source.port}" if source.host
      log "#{prefix}: #{source.dbname}#{location}"
    end

    def in_parallel(tables, first_schema:, &block)
      spinners = TTY::Spinner::Multi.new(format: :dots, output: output)
      item_spinners = {}

      start = lambda do |item, i|
        table, opts = item
        message = String.new(":spinner ")
        message << table.sub("#{first_schema}.", "")
        message << " #{opts[:sql]}" if opts[:sql]
        spinner = spinners.register(message)
        spinner.auto_spin
        item_spinners[item] = spinner
      end

      failed_tables = []

      finish = lambda do |item, i, result|
        spinner = item_spinners[item]
        table_name = item.first.sub("#{first_schema}.", "")

        if result[:status] == "success"
          spinner.success(display_message(result))
        else
          # TODO add option to fail fast
          spinner.error(display_message(result))
          failed_tables << table_name
          fail_sync(failed_tables) if @options[:fail_fast]
        end

        unless spinner.send(:tty?)
          status = result[:status] == "success" ? "✔" : "✖"
          log [status, table_name, item.last[:sql], display_message(result)].compact.join(" ")
        end
      end

      options = {start: start, finish: finish}
      if @options[:debug] || @options[:in_batches]
        options[:in_processes] = 0
      else
        options[:in_threads] = 4 if windows?
      end

      # could try to use `raise Parallel::Kill` to fail faster with --fail-fast
      # see `fast_faster` branch
      # however, need to make sure connections are cleaned up properly
      Parallel.each(tables, **options, &block)

      fail_sync(failed_tables) if failed_tables.any?
    end

    def fail_sync(failed_tables)
      raise Error, "Sync failed for #{failed_tables.size} table#{failed_tables.size == 1 ? nil : "s"}: #{failed_tables.join(", ")}"
    end

    def display_message(result)
      messages = []
      messages << "- #{result[:time]}s" if result[:time]
      messages << "(#{result[:message].lines.first.to_s.strip})" if result[:message]
      messages.join(" ")
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
    end

    def deprecated(message)
      log colorize("[DEPRECATED] #{message}", :yellow)
    end

    def log_completed(start_time)
      time = Time.now - start_time
      message = "Completed in #{time.round(1)}s"
      log colorize(message, :green)
    end

    def windows?
      Gem.win_platform?
    end
  end
end
