module PgSync
  class TableSync
    include Utils

    attr_reader :source, :destination, :tasks, :opts, :resolver

    def initialize(source:, destination:, tasks:, opts:, resolver:)
      @source = source
      @destination = destination
      @tasks = tasks
      @opts = opts
      @resolver = resolver
    end

    def perform
      confirm_tables_exist(destination, tasks, "destination")

      add_columns

      show_notes

      # don't sync tables with no shared fields
      # we show a warning message above
      run_tasks(tasks.reject { |task| task.shared_fields.empty? })
    end

    # TODO only query specific tables
    # TODO add sequences, primary keys, etc
    def add_columns
      source_columns = columns(source)
      destination_columns = columns(destination)

      tasks.each do |task|
        task.from_columns = source_columns[task.table] || []
        task.to_columns = destination_columns[task.table] || []
      end
    end

    def show_notes
      # for tables
      resolver.notes.each do |note|
        warning note
      end

      # for columns and sequences
      tasks.each do |task|
        task.notes.each do |note|
          warning "#{task_name(task)}: #{note}"
        end
      end

      # for non-deferrable constraints
      if opts[:defer_constraints]
        constraints = non_deferrable_constraints(destination)
        constraints = tasks.flat_map { |t| constraints[t.table] || [] }
        warning "Non-deferrable constraints: #{constraints.join(", ")}" if constraints.any?
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
      data_source.execute(query).group_by { |r| Table.new(r["schema"], r["table"]) }.map do |k, v|
        [k, v.map { |r| {name: r["column"], type: r["type"]} }]
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
      data_source.execute(query).group_by { |r| Table.new(r["schema"], r["table"]) }.map do |k, v|
        [k, v.map { |r| r["constraint_name"] }]
      end.to_h
    end

    def run_tasks(tasks, &block)
      notices = []
      failed_tables = []

      spinners = TTY::Spinner::Multi.new(format: :dots, output: output)
      task_spinners = {}
      started_at = {}

      start = lambda do |task, i|
        message = ":spinner #{display_item(task)}"
        spinner = spinners.register(message)
        if opts[:in_batches]
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
          spinner.error(message)
          failed_tables << task_name(task)
          fail_sync(failed_tables) if opts[:fail_fast]
        end

        unless spinner.send(:tty?)
          status = result[:status] == "success" ? "✔" : "✖"
          log [status, display_item(task), message].join(" ")
        end
      end

      options = {start: start, finish: finish}

      jobs = opts[:jobs]
      if opts[:debug] || opts[:in_batches] || opts[:defer_constraints]
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

          task.perform
        end
      end

      notices.each do |notice|
        warning notice
      end

      fail_sync(failed_tables) if failed_tables.any?
    end

    def maybe_defer_constraints
      if opts[:defer_constraints]
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

    def display_item(item)
      messages = []
      messages << task_name(item)
      messages << item.opts[:sql] if item.opts[:sql]
      messages.join(" ")
    end

    def windows?
      Gem.win_platform?
    end
  end
end
