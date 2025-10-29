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

      show_spinner = output.tty? && !opts[:debug]

      if show_spinner
        spinner = TTY::Spinner.new(":spinner Syncing schema", format: :dots)
        spinner.auto_spin
      end

      # if spinner, capture lines to show on error
      lines = []
      stdout, stderr, status =
        begin
          Open3.capture3(*dump_command)
        rescue Errno::ENOENT
          raise Error, "pg_dump not found"
        end
      success = status.success?
      stderr.each_line do |line|
        if show_spinner
          lines << line
        else
          log line
        end
      end

      @destination.transaction do
        create_schemas if specify_tables?
        @destination.conn.exec(stdout.gsub(/^\\(un)?restrict .+/, "").sub("SET transaction_timeout = 0;", ""))
      end
      # reset session variables
      @destination.send(:reconnect)

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

    def dump_command
      cmd = ["pg_dump", "--schema-only", "--no-owner", "--no-acl"]
      if specify_tables?
        @tasks.each do |task|
          cmd.concat(["-t", task.quoted_table])
        end
      end
      cmd.concat(["-d", @source.url])
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
