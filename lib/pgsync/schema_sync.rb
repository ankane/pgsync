module PgSync
  class SchemaSync
    include Utils

    attr_reader :args, :opts

    def initialize(source:, destination:, tasks:, args:, opts:)
      @source = source
      @destination = destination
      @tasks = tasks
      @args = args
      @opts = opts
    end

    def perform
      if opts[:preserve]
        raise Error, "Cannot use --preserve with --schema-first or --schema-only"
      end

      # generate commands before starting spinner
      # for better error output if pg_restore not found
      dump_command = dump_command()
      restore_command = restore_command()

      show_spinner = output.tty? && !opts[:debug]

      if show_spinner
        spinner = TTY::Spinner.new(":spinner Syncing schema", format: :dots)
        spinner.auto_spin
      end

      create_schemas if specify_tables?

      # if spinner, capture lines to show on error
      lines = []
      success =
        run_command(dump_command, restore_command) do |line|
          if show_spinner
            lines << line
          else
            log line
          end
        end

      if show_spinner
        if success
          spinner.success
        else
          spinner.error
          log lines.join
        end
      end

      raise Error, "Schema sync returned non-zero exit code" unless success
    end

    private

    def run_command(dump_command, restore_command)
      err_r, err_w = IO.pipe
      Open3.pipeline_start(dump_command, restore_command, err: err_w) do |wait_thrs|
        err_w.close
        err_r.each do |line|
          yield line
        end
        wait_thrs.all? { |t| t.value.success? }
      end
    end

    # --if-exists introduced in Postgres 9.4
    # not ideal, but simpler than trying to parse version
    def supports_if_exists?
      `pg_restore --help`.include?("--if-exists")
    rescue Errno::ENOENT
      raise Error, "pg_restore not found"
    end

    def dump_command
      cmd = ["pg_dump", "-Fc", "--verbose", "--schema-only", "--no-owner", "--no-acl"]
      if specify_tables?
        @tasks.each do |task|
          cmd.concat(["-t", task.quoted_table])
        end
      end
      cmd.concat(["-d", @source.url])
    end

    def restore_command
      cmd = ["pg_restore", "--verbose", "--no-owner", "--no-acl", "--clean"]
      cmd << "--if-exists" if supports_if_exists?
      cmd.concat(["-d", @destination.url])
    end

    # pg_dump -t won't create schemas (even with -n)
    # not ideal that this happens outside restore transaction
    def create_schemas
      schemas = @tasks.map { |t| t.table.schema }.uniq - @destination.schemas
      schemas.sort.each do |schema|
        @destination.create_schema(schema)
      end
    end

    def specify_tables?
      !opts[:all_schemas] || opts[:tables] || opts[:groups] || args[0] || opts[:exclude] || opts[:schemas]
    end
  end
end
