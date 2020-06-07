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

      show_spinner = output.tty? && !opts[:debug]

      if show_spinner
        spinner = TTY::Spinner.new(":spinner Syncing schema", format: :dots)
        spinner.auto_spin
      end

      lines = []
      success =
        run_command("#{dump_command} | #{restore_command}") do |line|
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
          puts lines.join
        end
      end

      raise Error, "Schema sync returned non-zero exit code" unless success
    end

    private

    def run_command(command)
      Open3.popen2e(command) do |stdin, stdout, wait_thr|
        stdout.each do |line|
          yield
        end
        wait_thr.value.success?
      end
    end

    def pg_restore_version
      `pg_restore --version`.lines[0].chomp.split(" ")[-1].split(/[^\d.]/)[0]
    rescue Errno::ENOENT
      raise Error, "pg_restore not found"
    end

    def dump_command
      tables =
        if opts[:tables] || opts[:groups] || args[0] || opts[:exclude]
          @tasks.map { |task| "-t #{Shellwords.escape(task.quoted_table)}" }
        else
          []
        end

      "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables.join(" ")} -d #{@source.url}"
    end

    def restore_command
      if_exists = Gem::Version.new(pg_restore_version) >= Gem::Version.new("9.4.0")
      "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{@destination.url}"
    end
  end
end
