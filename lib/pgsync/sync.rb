module PgSync
  class Sync
    include Utils

    def initialize(arguments, options)
      @arguments = arguments
      @options = options
    end

    def perform
      args = @arguments
      opts = @options

      # only resolve commands from config, not CLI arguments
      [:to, :from].each do |opt|
        opts[opt] ||= resolve_source(config[opt.to_s])
      end

      # merge other config
      [:to_safe, :exclude, :schemas].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end

      started_at = Time.now

      if args.size > 2
        raise Error, "Usage:\n    pgsync [options]"
      end

      raise Error, "No source" unless source.exists?
      raise Error, "No destination" unless destination.exists?

      unless opts[:to_safe] || destination.local?
        raise Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
      end

      if (opts[:preserve] || opts[:overwrite]) && destination.server_version_num < 90500
        raise Error, "Postgres 9.5+ is required for --preserve and --overwrite"
      end

      print_description("From", source)
      print_description("To", destination)

      resolver = TaskResolver.new(args: args, opts: opts, source: source, destination: destination, config: config, first_schema: first_schema)
      tasks =
        resolver.tasks.map do |task|
          Task.new(source: source, destination: destination, config: config, table: task[:table], opts: opts.merge(sql: task[:sql]))
        end

      if opts[:in_batches] && tasks.size > 1
        raise Error, "Cannot use --in-batches with multiple tables"
      end

      confirm_tables_exist(source, tasks, "source")

      # TODO remove?
      if opts[:list]
        confirm_tables_exist(destination, tasks, "destination")
        pretty_list tasks.map { |task| task_name(task) }
      else
        if opts[:schema_first] || opts[:schema_only]
          if opts[:preserve]
            raise Error, "Cannot use --preserve with --schema-first or --schema-only"
          end

          log "* Dumping schema"
          schema_tasks =
            if opts[:tables] || opts[:groups] || args[0] || opts[:exclude]
              tasks
            end
          SchemaSync.new(source: source, destination: destination, tasks: schema_tasks).perform
        end

        unless opts[:schema_only]
          confirm_tables_exist(destination, tasks, "destination")

          # TODO only query specific tables
          # TODO add sequences, primary keys, etc
          source_columns = columns(source)
          destination_columns = columns(destination)

          tasks.each do |task|
            task.from_columns = source_columns[task.table] || []
            task.to_columns = destination_columns[task.table] || []
          end

          # show notes before we start
          resolver.notes.each do |note|
            warning note
          end
          tasks.each do |task|
            task.notes.each do |note|
              warning "#{task_name(task)}: #{note}"
            end
          end
          if opts[:defer_constraints]
            constraints = non_deferrable_constraints(destination)
            constraints = tasks.flat_map { |t| constraints[t.table] || [] }
            warning "Non-deferrable constraints: #{constraints.join(", ")}" if constraints.any?
          end

          # don't sync tables with no shared fields
          # we show a warning message above
          tasks.reject! { |task| task.shared_fields.empty? }

          in_parallel(tasks) do |task|
            task.perform
          end
        end

        log_completed(started_at)
      end
    end

    def columns(data_source)
      query = <<~SQL
        SELECT
          table_schema AS schema,
          table_name AS table,
          column_name AS column,
          data_type AS type
        FROM
          information_schema.columns
        ORDER BY 1, 2, 3
      SQL
      data_source.execute(query).group_by { |r| [r["schema"], r["table"]] }.map do |k, v|
        [k.join("."), v.map { |r| {name: r["column"], type: r["type"]} }]
      end.to_h
    end

    def non_deferrable_constraints(data_source)
      query = <<~SQL
        SELECT
          table_schema AS schema,
          table_name AS table,
          constraint_name
        FROM
          information_schema.table_constraints
        WHERE
          constraint_type = 'FOREIGN KEY' AND
          is_deferrable = 'NO'
      SQL
      data_source.execute(query).group_by { |r| [r["schema"], r["table"]] }.map do |k, v|
        [k.join("."), v.map { |r| r["constraint_name"] }]
      end.to_h
    end

    def first_schema
      @first_schema ||= source.search_path.find { |sp| sp != "pg_catalog" }
    end

    def confirm_tables_exist(data_source, tasks, description)
      tasks.map(&:table).each do |table|
        unless data_source.table_exists?(table)
          raise Error, "Table not found in #{description}: #{table}"
        end
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
          rescue Errno::ENOENT
            raise Error, "Config file not found: #{file}"
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

    def in_parallel(tasks, &block)
      notices = []
      failed_tables = []

      spinners = TTY::Spinner::Multi.new(format: :dots, output: output)
      task_spinners = {}
      started_at = {}

      start = lambda do |task, i|
        message = ":spinner #{display_item(task)}"
        spinner = spinners.register(message)
        if @options[:in_batches]
          # log instead of spin for non-tty
          log message.sub(":spinner", "⠋")
        else
          spinner.auto_spin
        end
        task_spinners[task] = spinner
        started_at[task] = Time.now
      end

      finish = lambda do |task, i, result|
        spinner = task_spinners[task]
        time = (Time.now - started_at[task]).round(1)

        message =
          if result[:message]
            "(#{result[:message].lines.first.to_s.strip})"
          else
            "- #{time}s"
          end

        notices.concat(result[:notices])

        if result[:status] == "success"
          spinner.success(message)
        else
          # TODO add option to fail fast
          spinner.error(message)
          failed_tables << task_name(task)
          fail_sync(failed_tables) if @options[:fail_fast]
        end

        unless spinner.send(:tty?)
          status = result[:status] == "success" ? "✔" : "✖"
          log [status, display_item(task), message].join(" ")
        end
      end

      options = {start: start, finish: finish}

      jobs = @options[:jobs]
      if @options[:debug] || @options[:in_batches] || @options[:defer_constraints]
        warning "--jobs ignored" if jobs
        jobs = 0
      end

      if windows?
        options[:in_threads] = jobs || 4
      else
        options[:in_processes] = jobs if jobs
      end

      maybe_defer_constraints do
        # could try to use `raise Parallel::Kill` to fail faster with --fail-fast
        # see `fast_faster` branch
        # however, need to make sure connections are cleaned up properly
        Parallel.each(tasks, **options) do |task|
          source.reconnect_if_needed
          destination.reconnect_if_needed

          yield task
        end
      end

      notices.each do |notice|
        warning notice
      end

      fail_sync(failed_tables) if failed_tables.any?
    end

    def maybe_defer_constraints
      if @options[:defer_constraints]
        destination.transaction do
          destination.execute("SET CONSTRAINTS ALL DEFERRED")

          # create a transaction on the source
          # to ensure we get a consistent snapshot
          source.transaction do
            yield
          end
        end
      else
        yield
      end
    end

    def fail_sync(failed_tables)
      raise Error, "Sync failed for #{failed_tables.size} table#{failed_tables.size == 1 ? nil : "s"}: #{failed_tables.join(", ")}"
    end

    def task_name(task)
      task.table.sub("#{first_schema}.", "")
    end

    def display_item(item)
      messages = []
      messages << task_name(item)
      messages << item.opts[:sql] if item.opts[:sql]
      messages.join(" ")
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
    end

    def log_completed(started_at)
      time = Time.now - started_at
      message = "Completed in #{time.round(1)}s"
      log colorize(message, :green)
    end

    def windows?
      Gem.win_platform?
    end

    def source
      @source ||= data_source(@options[:from])
    end

    def destination
      @destination ||= data_source(@options[:to])
    end

    def data_source(url)
      ds = DataSource.new(url)
      ObjectSpace.define_finalizer(self, self.class.finalize(ds))
      ds
    end

    def resolve_source(source)
      if source
        source = source.dup
        source.gsub!(/\$\([^)]+\)/) do |m|
          command = m[2..-2]
          result = `#{command}`.chomp
          unless $?.success?
            raise Error, "Command exited with non-zero status:\n#{command}"
          end
          result
        end
      end
      source
    end

    def self.finalize(ds)
      # must use proc instead of stabby lambda
      proc { ds.close }
    end
  end
end
