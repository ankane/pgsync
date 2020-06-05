module PgSync
  class TaskResolver
    include Utils

    attr_reader :args, :opts, :source, :destination, :config, :first_schema, :notes

    def initialize(args:, opts:, source:, destination:, config:, first_schema:)
      @args = args
      @opts = opts
      @source = source
      @destination = destination
      @config = config
      @groups = config["groups"] || {}
      @first_schema = first_schema
      @notes = []
    end

    def tasks
      tasks = []

      # get lists from args
      groups, tables = process_args

      # expand groups into tasks
      groups.each do |group|
        tasks.concat(group_to_tasks(group))
      end

      # expand tables into tasks
      tables.each do |table|
        tasks.concat(table_to_tasks(table))
      end

      # get default if none given
      if !opts[:groups] && !opts[:tables] && args.size == 0
        tasks.concat(default_tasks)
      end

      # resolve any tables that need it
      tasks.each do |task|
        task[:table] = fully_resolve(task[:table])
      end

      tasks
    end

    def group?(group)
      @groups.key?(group)
    end

    private

    def group_to_tasks(value)
      group, param = value.split(":", 2)
      raise Error, "Group not found: #{group}" unless group?(group)

      @groups[group].map do |table|
        table_sql = nil
        if table.is_a?(Array)
          table, table_sql = table
        end

        {
          table: to_table(table),
          sql: expand_sql(table_sql, param)
        }
      end
    end

    def table_to_tasks(value)
      raise Error, "Cannot use parameters with tables" if value.include?(":")

      tables =
        if value.include?("*")
          regex = Regexp.new('\A' + Regexp.escape(value).gsub('\*','[^\.]*') + '\z')
          shared_tables.select { |t| regex.match(t.full_name) || regex.match(t.name) }
        else
          [to_table(value)]
        end

      tables.map do |table|
        {
          table: table,
          sql: sql_arg # doesn't support params
        }
      end
    end

    # treats identifiers as if they were quoted (Users == "Users")
    # this is different from Postgres (Users == "users")
    #
    # TODO add support for quoted identifiers like "my.schema"."my.table"
    # so it's possible to specify identifiers with "." in them
    def to_table(value)
      parts = value.split(".")
      case parts.size
      when 1
        # unknown schema
        Table.new(nil, parts[0])
      when 2
        Table.new(*parts)
      else
        raise Error, "Cannot resolve table: #{value}"
      end
    end

    def default_tasks
      shared_tables.map do |table|
        {
          table: table
        }
      end
    end

    # tables that exists in both source and destination
    # used when no tables specified, or a wildcard
    # removes excluded tables and filters by schema
    def shared_tables
      exclude = to_arr(opts[:exclude]).map { |t| fully_resolve(to_table(t)) }

      tables = source.tables
      unless opts[:schema_only] || opts[:schema_first]
        from_tables = tables
        to_tables = destination.tables

        extra_tables = to_tables - from_tables
        notes << "Extra tables: #{extra_tables.map { |t| friendly_name(t) }.join(", ")}" if extra_tables.any?

        missing_tables = from_tables - to_tables
        notes << "Missing tables: #{missing_tables.map { |t| friendly_name(t) }.join(", ")}" if missing_tables.any?

        tables &= to_tables
      end

      if opts[:schemas]
        schemas = Set.new(to_arr(opts[:schemas]))
        tables.select! { |t| schemas.include?(t.schema) }
      end

      tables - exclude
    end

    def process_args
      groups = to_arr(opts[:groups])
      tables = to_arr(opts[:tables])
      if args[0]
        # could be a group, table, or mix
        to_arr(args[0]).each do |value|
          if group?(value.split(":", 2)[0])
            groups << value
          else
            tables << value
          end
        end
      end
      [groups, tables]
    end

    def no_schema_tables
      @no_schema_tables ||= begin
        search_path_index = source.search_path.map.with_index.to_h
        source.tables.group_by(&:name).map do |group, t2|
          [group, t2.select { |t| search_path_index[t.schema] }.sort_by { |t| search_path_index[t.schema] }.first]
        end.to_h
      end
    end

    # for tables without a schema, find the table in the search path
    def fully_resolve(table)
      return table if table.schema
      no_schema_tables[table.name] || (raise Error, "Table not found in source: #{table.name}")
    end

    # parse command line arguments and YAML
    def to_arr(value)
      if value.is_a?(Array)
        value
      else
        # Split by commas, but don't use commas inside double quotes
        # https://stackoverflow.com/questions/21105360/regex-find-comma-not-inside-quotes
        value.to_s.split(/(?!\B"[^"]*),(?![^"]*"\B)/)
      end
    end

    def sql_arg
      args[1]
    end

    def expand_sql(sql, param)
      # command line option takes precedence over group option
      sql = sql_arg if sql_arg

      return unless sql

      # vars must match \w
      missing_vars = sql.scan(/{\w+}/).map { |v| v[1..-2] }

      vars = {}
      if param
        vars["id"] = cast(param)
        vars["1"] = cast(param)
      end

      sql = sql.dup
      vars.each do |k, v|
        # only sub if in var list
        sql.gsub!("{#{k}}", cast(v)) if missing_vars.delete(k)
      end

      raise Error, "Missing variables: #{missing_vars.uniq.join(", ")}" if missing_vars.any?

      sql
    end

    # TODO quote vars in next major version
    def cast(value)
      value.to_s.gsub(/\A\"|\"\z/, '')
    end
  end
end
