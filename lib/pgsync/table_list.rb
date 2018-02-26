module PgSync
  class TableList
    attr_reader :args, :opts, :source, :config

    def initialize(args, options, source, config)
      @args = args
      @opts = options
      @source = source
      @config = config
    end

    def tables
      tables = nil

      if opts[:groups]
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        specified_groups = to_arr(opts[:groups])
        specified_groups.map do |tag|
          group, id = tag.split(":", 2)
          if (t = (config["groups"] || {})[group])
            add_tables(tables, t, id, args[1])
          else
            raise PgSync::Error, "Group not found: #{group}"
          end
        end
      end

      if opts[:tables]
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        to_arr(opts[:tables]).each do |tag|
          table, id = tag.split(":", 2)
          add_table(tables, table, id, args[1])
        end
      end

      if args[0]
        # could be a group, table, or mix
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        specified_groups = to_arr(args[0])
        specified_groups.map do |tag|
          group, id = tag.split(":", 2)
          if (t = (config["groups"] || {})[group])
            add_tables(tables, t, id, args[1])
          else
            add_table(tables, group, id, args[1])
          end
        end
      end

      tables ||= Hash[(source.tables - to_arr(opts[:exclude])).map { |k| [k, {}] }]

      tables.keys.each do |table|
        unless source.table_exists?(table)
          raise PgSync::Error, "Table does not exist in source: #{table}"
        end
      end

      tables
    end

    private

    def to_arr(value)
      if value.is_a?(Array)
        value
      else
        # Split by commas, but don't use commas inside double quotes
        # http://stackoverflow.com/questions/21105360/regex-find-comma-not-inside-quotes
        value.to_s.split(/(?!\B"[^"]*),(?![^"]*"\B)/)
      end
    end

    def add_tables(tables, t, id, boom)
      t.each do |table|
        sql = nil
        if table.is_a?(Array)
          table, sql = table
        end
        add_table(tables, table, id, boom || sql)
      end
    end

    def add_table(tables, table, id, boom, wildcard = false)
      if table.include?("*") && !wildcard
        regex = Regexp.new('\A' + Regexp.escape(table).gsub('\*','[^\.]*') + '\z')
        t2 = source.tables.select { |t| regex.match(t) }
        t2.each do |tab|
          add_table(tables, tab, id, boom, true)
        end
      else
        tables[table] = {}
        tables[table][:sql] = boom.gsub("{id}", cast(id)).gsub("{1}", cast(id)) if boom
      end
    end

    def cast(value)
      value.to_s.gsub(/\A\"|\"\z/, '')
    end
  end
end
