module PgSync
  class TaskResolver
    include Utils

    attr_reader :args, :opts, :source, :config

    def initialize(args, options, source, config)
      @args = args
      @opts = options
      @source = source
      @config = config
      @groups = config["groups"] || {}
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
          table: table,
          sql: expand_sql(table_sql, param)
        }
      end
    end

    def table_to_tasks(table)
      raise Error, "Cannot use parameters with tables" if table.include?(":")

      tables =
        if table.include?("*")
          regex = Regexp.new('\A' + Regexp.escape(table).gsub('\*','[^\.]*') + '\z')
          source.tables.select { |t| regex.match(t) || regex.match(t.split(".", 2).last) }
        else
          [table]
        end

      tables.map do |table|
        {
          table: table,
          sql: sql_arg # doesn't support params
        }
      end
    end

    def default_tasks
      exclude = to_arr(opts[:exclude]).map { |t| fully_resolve(t) }

      tables = source.tables
      unless opts[:all_schemas]
        # only get tables in schema / search path
        schemas = Set.new(opts[:schemas] ? to_arr(opts[:schemas]) : source.search_path)
        tables.select! { |t| schemas.include?(t.split(".", 2)[0]) }
      end

      (tables - exclude).map do |table|
        {
          table: table
        }
      end
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
        source.tables.group_by { |t| t.split(".", 2)[-1] }.map do |group, t2|
          [group, t2.sort_by { |t| [search_path_index[t.split(".", 2)[0]] || 1000000, t] }[0]]
        end.to_h
      end
    end

    def fully_resolve(table)
      return table if table.include?(".")
      no_schema_tables[table] || (raise Error, "Table not found in source: #{table}")
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
