require "pgsync/version"
require "yaml"
require "slop"
require "uri"
require "erb"
require "pg"
require "parallel"
require "multiprocessing"
require "fileutils"
require "tempfile"
require "cgi"

module URI
  class POSTGRESQL < Generic
    DEFAULT_PORT = 5432
  end
  @@schemes["POSTGRESQL"] = @@schemes["POSTGRES"] = POSTGRESQL
end

module PgSync
  class Error < StandardError; end

  class Client
    def initialize(args)
      $stdout.sync = true
      @exit = false
      @arguments, @options = parse_args(args)
      @mutex = MultiProcessing::Mutex.new
    end

    # TODO clean up this mess
    def perform
      return if @exit

      start_time = Time.now

      args, opts = @arguments, @options
      [:to, :from, :to_safe, :exclude].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end
      command = args[0]

      case command
      when "setup"
        args.shift
        opts[:setup] = true
        deprecated "Use `psync --setup` instead"
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

      if opts[:setup]
        setup(db_config_file(args[0]) || config_file || ".pgsync.yml")
      else
        if args.size > 2
          abort "Usage:\n    pgsync [options]"
        end

        source = parse_source(opts[:from])
        abort "No source" unless source
        source_uri, from_schema = parse_uri(source)

        destination = parse_source(opts[:to])
        abort "No destination" unless destination
        destination_uri, to_schema = parse_uri(destination)
        abort "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1" unless %(localhost 127.0.0.1).include?(destination_uri.host) || opts[:to_safe]

        print_uri("From", source_uri)
        print_uri("To", destination_uri)

        from_uri = source_uri
        to_uri = destination_uri

        tables = table_list(args, opts, from_uri, from_schema)

        if opts[:schema_only]
          log "* Dumping schema"
          tables = tables.keys.map { |t| "-t #{t}" }.join(" ")
          psql_version = Gem::Version.new(`psql --version`.lines[0].chomp.split(" ")[-1])
          if_exists = psql_version >= Gem::Version.new("9.4.0")
          dump_command = "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables} #{to_url(source_uri)}"
          restore_command = "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{to_url(destination_uri)}"
          system("#{dump_command} | #{restore_command}")

          log_completed(start_time)
        else
          with_connection(to_uri, timeout: 3) do |conn|
            tables.keys.each do |table|
              unless table_exists?(conn, table, to_schema)
                abort "Table does not exist in destination: #{table}"
              end
            end
          end

          if opts[:list]
            if args[0] == "groups"
              pretty_list (config["groups"] || {}).keys
            else
              pretty_list tables.keys
            end
          else
            in_parallel(tables) do |table, table_opts|
              sync_table(table, opts.merge(table_opts), from_uri, to_uri, from_schema, to_schema)
            end

            log_completed(start_time)
          end
        end
      end
      true
    end

    protected

    def sync_table(table, opts, from_uri, to_uri, from_schema, to_schema)
      time =
        benchmark do
          with_connection(from_uri) do |from_connection|
            with_connection(to_uri) do |to_connection|
              bad_fields = opts[:no_rules] ? [] : config["data_rules"]

              from_fields = columns(from_connection, table, from_schema)
              to_fields = columns(to_connection, table, to_schema)
              shared_fields = to_fields & from_fields
              extra_fields = to_fields - from_fields
              missing_fields = from_fields - to_fields

              from_sequences = sequences(from_connection, table, shared_fields)
              to_sequences = sequences(to_connection, table, shared_fields)
              shared_sequences = to_sequences & from_sequences
              extra_sequences = to_sequences - from_sequences
              missing_sequences = from_sequences - to_sequences

              sql_clause = String.new

              @mutex.synchronize do
                log "* Syncing #{table}"
                if opts[:sql]
                  log "    #{opts[:sql]}"
                  sql_clause << " #{opts[:sql]}"
                end
                log "    Extra columns: #{extra_fields.join(", ")}" if extra_fields.any?
                log "    Missing columns: #{missing_fields.join(", ")}" if missing_fields.any?
                log "    Extra sequences: #{extra_sequences.join(", ")}" if extra_sequences.any?
                log "    Missing sequences: #{missing_sequences.join(", ")}" if missing_sequences.any?

                if shared_fields.empty?
                  log "    No fields to copy"
                end
              end

              if shared_fields.any?
                copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, bk| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f, from_connection)} AS #{escape_identifier(f)}" : "#{table}.#{escape_identifier(f)}" }.join(", ")
                fields = shared_fields.map { |f| escape_identifier(f) }.join(", ")

                seq_values = {}
                shared_sequences.each do |seq|
                  seq_values[seq] = from_connection.exec("select last_value from #{seq}").to_a[0]["last_value"]
                end

                copy_to_command = "COPY (SELECT #{copy_fields} FROM #{table}#{sql_clause}) TO STDOUT"
                if opts[:in_batches]
                  primary_key = self.primary_key(from_connection, table, from_schema)
                  abort "No primary key" unless primary_key

                  from_max_id = max_id(from_connection, table, primary_key, sql_clause)
                  to_max_id = max_id(to_connection, table, primary_key, sql_clause) + 1

                  if to_max_id == 1
                    from_min_id = min_id(from_connection, table, primary_key, sql_clause)
                    to_max_id = from_min_id if from_min_id > 0
                  end

                  starting_id = to_max_id
                  batch_size = opts[:batch_size]

                  i = 1
                  batch_count = ((from_max_id - starting_id + 1) / batch_size.to_f).ceil

                  while starting_id <= from_max_id
                    where = "#{primary_key} >= #{starting_id} AND #{primary_key} < #{starting_id + batch_size}"
                    log "    #{i}/#{batch_count}: #{where}"

                    # TODO be smarter for advance sql clauses
                    batch_sql_clause = " #{sql_clause.length > 0 ? "#{sql_clause} AND" : "WHERE"} #{where}"

                    batch_copy_to_command = "COPY (SELECT #{copy_fields} FROM #{table}#{batch_sql_clause}) TO STDOUT"
                    to_connection.copy_data "COPY #{table} (#{fields}) FROM STDIN" do
                      from_connection.copy_data batch_copy_to_command do
                        while row = from_connection.get_copy_data
                          to_connection.put_copy_data(row)
                        end
                      end
                    end

                    starting_id += batch_size
                    i += 1

                    if opts[:sleep] && starting_id <= from_max_id
                      sleep(opts[:sleep])
                    end
                  end
                elsif !opts[:truncate] && (opts[:overwrite] || opts[:preserve] || !sql_clause.empty?)
                  primary_key = self.primary_key(to_connection, table, to_schema)
                  abort "No primary key" unless primary_key

                  temp_table = "pgsync_#{rand(1_000_000_000)}"
                  file = Tempfile.new(temp_table)
                  begin
                    from_connection.copy_data copy_to_command do
                      while row = from_connection.get_copy_data
                        file.write(row)
                      end
                    end
                    file.rewind

                    to_connection.transaction do
                      # create a temp table
                      to_connection.exec("CREATE TABLE #{temp_table} AS SELECT * FROM #{table} WITH NO DATA")

                      # load file
                      to_connection.copy_data "COPY #{temp_table} (#{fields}) FROM STDIN" do
                        file.each do |row|
                          to_connection.put_copy_data(row)
                        end
                      end

                      if opts[:preserve]
                        # insert into
                        to_connection.exec("INSERT INTO #{table} (SELECT * FROM #{temp_table} WHERE NOT EXISTS (SELECT 1 FROM #{table} WHERE #{table}.#{primary_key} = #{temp_table}.#{primary_key}))")
                      else
                        to_connection.exec("DELETE FROM #{table} WHERE #{primary_key} IN (SELECT #{primary_key} FROM #{temp_table})")
                        to_connection.exec("INSERT INTO #{table} (SELECT * FROM #{temp_table})")
                      end

                      # delete temp table
                      to_connection.exec("DROP TABLE #{temp_table}")
                    end
                  ensure
                     file.close
                     file.unlink
                  end
                else
                  to_connection.exec("TRUNCATE #{table} CASCADE")
                  to_connection.copy_data "COPY #{table} (#{fields}) FROM STDIN" do
                    from_connection.copy_data copy_to_command do
                      while row = from_connection.get_copy_data
                        to_connection.put_copy_data(row)
                      end
                    end
                  end
                end
                seq_values.each do |seq, value|
                  to_connection.exec("SELECT setval(#{escape(seq)}, #{escape(value)})")
                end
              end
            end
          end
        end

      @mutex.synchronize do
        log "* DONE #{table} (#{time.round(1)}s)"
      end
    end

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = %{Usage:
    pgsync [options]

Options:}
        o.string "-t", "--tables", "tables"
        o.string "-g", "--groups", "groups"
        o.string "-d", "--db", "database"
        o.string "--from", "source"
        o.string "--to", "destination"
        o.string "--where", "where", help: false
        o.integer "--limit", "limit", help: false
        o.string "--exclude", "exclude tables"
        o.string "--config", "config file"
        # TODO much better name for this option
        o.boolean "--to-safe", "accept danger", default: false
        o.boolean "--debug", "debug", default: false
        o.boolean "--list", "list", default: false
        o.boolean "--overwrite", "overwrite existing rows", default: false, help: false
        o.boolean "--preserve", "preserve existing rows", default: false
        o.boolean "--truncate", "truncate existing rows", default: false
        o.boolean "--schema-only", "schema only", default: false
        o.boolean "--no-rules", "do not apply data rules", default: false
        o.boolean "--setup", "setup", default: false
        o.boolean "--in-batches", "in batches", default: false, help: false
        o.integer "--batch-size", "batch size", default: 10000, help: false
        o.float "--sleep", "sleep", default: 0, help: false
        o.on "-v", "--version", "print the version" do
          log PgSync::VERSION
          @exit = true
        end
        o.on "-h", "--help", "prints help" do
          log o
          @exit = true
        end
      end
      [opts.arguments, opts.to_hash]
    rescue Slop::Error => e
      abort e.message
    end

    def config
      @config ||= begin
        if config_file
          begin
            YAML.load_file(config_file) || {}
          rescue Psych::SyntaxError => e
            raise PgSync::Error, e.message
          end
        else
          {}
        end
      end
    end

    def parse_source(source)
      if source && source[0..1] == "$(" && source[-1] == ")"
        command = source[2..-2]
        # log "Running #{command}"
        source = `#{command}`.chomp
        unless $?.success?
          abort "Command exited with non-zero status:\n#{command}"
        end
      end
      source
    end

    def setup(config_file)
      if File.exist?(config_file)
        abort "#{config_file} exists."
      else
        FileUtils.cp(File.dirname(__FILE__) + "/../config.yml", config_file)
        log "#{config_file} created. Add your database credentials."
      end
    end

    def db_config_file(db)
      return unless db
      ".pgsync-#{db}.yml"
    end

    # borrowed from
    # ActiveRecord::ConnectionAdapters::ConnectionSpecification::ConnectionUrlResolver
    def with_connection(uri, timeout: 0)
      uri_parser = URI::Parser.new
      config = {
          host: uri.host,
          port: uri.port,
          dbname: uri.path.sub(/\A\//, ""),
          user: uri.user,
          password: uri.password,
          connect_timeout: timeout
      }.reject { |_, value| value.to_s.empty? }
      config.map { |key, value| config[key] = uri_parser.unescape(value) if value.is_a?(String) }
      conn = PG::Connection.new(config)
      begin
        yield conn
      ensure
        conn.close
      end
    rescue PG::ConnectionBad => e
      log
      abort e.message
    end

    def benchmark
      start_time = Time.now
      yield
      Time.now - start_time
    end

    def tables(conn, schema)
      query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = $1 ORDER BY tablename ASC"
      conn.exec_params(query, [schema]).to_a.map { |row| row["tablename"] }
    end

    def columns(conn, table, schema)
      query = "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2"
      conn.exec_params(query, [schema, table]).to_a.map { |row| row["column_name"] }
    end

    def table_exists?(conn, table, schema)
      query = "SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2"
      conn.exec_params(query, [schema, table]).to_a.size > 0
    end

    # http://stackoverflow.com/a/20537829
    def primary_key(conn, table, schema)
      query = <<-SQL
        SELECT
          pg_attribute.attname,
          format_type(pg_attribute.atttypid, pg_attribute.atttypmod)
        FROM
          pg_index, pg_class, pg_attribute, pg_namespace
        WHERE
          pg_class.oid = $2::regclass AND
          indrelid = pg_class.oid AND
          nspname = $1 AND
          pg_class.relnamespace = pg_namespace.oid AND
          pg_attribute.attrelid = pg_class.oid AND
          pg_attribute.attnum = any(pg_index.indkey) AND
          indisprimary
      SQL
      row = conn.exec_params(query, [schema, table]).to_a[0]
      row && row["attname"]
    end

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table}.#{column}")
    end

    # TODO wildcard rules
    def apply_strategy(rule, table, column, conn)
      if rule.is_a?(Hash)
        if rule.key?("value")
          escape(rule["value"])
        elsif rule.key?("statement")
          rule["statement"]
        else
          abort "Unknown rule #{rule.inspect} for column #{column}"
        end
      else
        strategies = {
          "unique_email" => "'email' || #{table}.id || '@example.org'",
          "untouched" => escape_identifier(column),
          "unique_phone" => "(#{table}.id + 1000000000)::text",
          "random_int" => "(RAND() * 10)::int",
          "random_date" => "'1970-01-01'",
          "random_time" => "NOW()",
          "unique_secret" => "'secret' || #{table}.id",
          "random_ip" => "'127.0.0.1'",
          "random_letter" => "'A'",
          "null" => "NULL",
          nil => "NULL"
        }
        if strategies[rule]
          strategies[rule]
        else
          abort "Unknown rule #{rule} for column #{column}"
        end
      end
    end

    def escape_identifier(value)
      PG::Connection.quote_ident(value)
    end

    def escape(value)
      if value.is_a?(String)
        "'#{quote_string(value)}'"
      else
        value
      end
    end

    # activerecord
    def quote_string(s)
      s.gsub(/\\/, '\&\&').gsub(/'/, "''")
    end

    def to_arr(value)
      if value.is_a?(Array)
        value
      else
        # Split by commas, but don't use commas inside double quotes
        # http://stackoverflow.com/questions/21105360/regex-find-comma-not-inside-quotes
        value.to_s.split(/(?!\B"[^"]*),(?![^"]*"\B)/)
      end
    end

    def parse_uri(url)
      uri = URI.parse(url)
      uri.scheme ||= "postgres"
      uri.host ||= "localhost"
      uri.port ||= 5432
      uri.path = "/#{uri.path}" if uri.path && uri.path[0] != "/"
      schema = CGI.parse(uri.query.to_s)["schema"][0] || "public"
      [uri, schema]
    end

    def print_uri(prefix, uri)
      log "#{prefix}: #{uri.path.sub(/\A\//, '')} on #{uri.host}:#{uri.port}"
    end

    def to_url(uri)
      uri = uri.dup
      uri.query = nil
      uri.to_s
    end

    def search_tree(file)
      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        if File.exist?(absolute_file)
          break absolute_file
        end
        path = File.dirname(path)
        break if path == "/"
      end
    end

    def config_file
      return @config_file if instance_variable_defined?(:@config_file)

      @config_file =
        search_tree(
          if @options[:db]
            db_config_file(@options[:db])
          else
            @options[:config] || ".pgsync.yml"
          end
        )
    end

    def abort(message)
      raise PgSync::Error, message
    end

    def log(message = nil)
      $stderr.puts message
    end

    def sequences(conn, table, columns)
      conn.exec("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape(table)}, #{escape(f)}) AS #{f}" }.join(", ")}").to_a[0].values.compact
    end

    def in_parallel(tables, &block)
      if @options[:debug] || @options[:in_batches]
        tables.each(&block)
      else
        Parallel.each(tables, &block)
      end
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
    end

    def add_tables(tables, t, id, boom, from_uri, from_schema)
      t.each do |table|
        sql = nil
        if table.is_a?(Array)
          table, sql = table
        end
        add_table(tables, table, id, boom || sql, from_uri, from_schema)
      end
    end

    def add_table(tables, table, id, boom, from_uri, from_schema, wildcard = false)
      if table.include?("*") && !wildcard
        regex = Regexp.new('\A' + Regexp.escape(table).gsub('\*','[^\.]*') + '\z')
        t2 = with_connection(from_uri) { |conn| self.tables(conn, from_schema) }.select { |t| regex.match(t) }
        t2.each do |tab|
          add_table(tables, tab, id, boom, from_uri, from_schema, true)
        end
      else
        tables[table] = {}
        tables[table][:sql] = boom.gsub("{id}", cast(id)).gsub("{1}", cast(id)) if boom
      end
    end

    def table_list(args, opts, from_uri, from_schema)
      tables = nil

      if opts[:groups]
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        specified_groups = to_arr(opts[:groups])
        specified_groups.map do |tag|
          group, id = tag.split(":", 2)
          if (t = (config["groups"] || {})[group])
            add_tables(tables, t, id, args[1], from_uri, from_schema)
          else
            abort "Group not found: #{group}"
          end
        end
      end

      if opts[:tables]
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        to_arr(opts[:tables]).each do |tag|
          table, id = tag.split(":", 2)
          add_table(tables, table, id, args[1], from_uri, from_schema)
        end
      end

      if args[0]
        # could be a group, table, or mix
        tables ||= Hash.new { |hash, key| hash[key] = {} }
        specified_groups = to_arr(args[0])
        specified_groups.map do |tag|
          group, id = tag.split(":", 2)
          if (t = (config["groups"] || {})[group])
            add_tables(tables, t, id, args[1], from_uri, from_schema)
          else
            add_table(tables, group, id, args[1], from_uri, from_schema)
          end
        end
      end

      with_connection(from_uri, timeout: 3) do |conn|
        tables ||= Hash[(self.tables(conn, from_schema) - to_arr(opts[:exclude])).map { |k| [k, {}] }]

        tables.keys.each do |table|
          unless table_exists?(conn, table, from_schema)
            abort "Table does not exist in source: #{table}"
          end
        end
      end

      tables
    end

    def max_id(conn, table, primary_key, sql_clause = nil)
      conn.exec("SELECT MAX(#{escape_identifier(primary_key)}) FROM #{escape_identifier(table)}#{sql_clause}").to_a[0]["max"].to_i
    end

    def min_id(conn, table, primary_key, sql_clause = nil)
      conn.exec("SELECT MIN(#{escape_identifier(primary_key)}) FROM #{escape_identifier(table)}#{sql_clause}").to_a[0]["min"].to_i
    end

    def cast(value)
      value.to_s.gsub(/\A\"|\"\z/, '')
    end

    def deprecated(message)
      log "[DEPRECATED] #{message}"
    end

    def log_completed(start_time)
      time = Time.now - start_time
      log "Completed in #{time.round(1)}s"
    end
  end
end
