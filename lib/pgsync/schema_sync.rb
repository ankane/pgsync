module PgSync
  class SchemaSync
    def initialize(source:, destination:, tasks:)
      @source = source
      @destination = destination
      @tasks = tasks
    end

    def perform
      unless system("#{dump_command} | #{restore_command}")
        raise Error, "Schema sync returned non-zero exit code"
      end
    end

    private

    def pg_restore_version
      `pg_restore --version`.lines[0].chomp.split(" ")[-1].split(/[^\d.]/)[0]
    rescue Errno::ENOENT
      raise Error, "pg_restore not found"
    end

    def dump_command
      tables = @tasks ? @tasks.map { |task| "-t #{Shellwords.escape(task.quoted_table)}" }.join(" ") : ""
      "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables} -d #{@source.url}"
    end

    def restore_command
      if_exists = Gem::Version.new(pg_restore_version) >= Gem::Version.new("9.4.0")
      "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{@destination.url}"
    end
  end
end
