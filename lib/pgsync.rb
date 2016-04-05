require "pgsync/version"
require "yaml"
require "slop"
require "uri"
require "erb"
require "pg"
require "parallel"
require "multiprocessing"
require "fileutils"

module URI
  class POSTGRESQL < Generic
    DEFAULT_PORT = 5432
  end
  @@schemes["POSTGRESQL"] = @@schemes["POSTGRES"] = POSTGRESQL
end

module PgSync
  class Error < StandardError; end
  class Rollback < StandardError; end

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

      if command == "setup"
        setup(db_config_file(args[1]) || config_file)
      else
        source = parse_source(opts[:from])
        abort "No source" unless source
        source_uri = parse_uri(source)

        destination = parse_source(opts[:to])
        abort "No destination" unless destination
        destination_uri = parse_uri(destination)
        abort "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1" unless %(localhost 127.0.0.1).include?(destination_uri.host) || opts[:to_safe]

        print_uri("From", source_uri)
        print_uri("To", destination_uri)

        if args[0] == "schema"
          time =
            benchmark do
              log "* Dumping schema"
              tables = to_arr(args[1]).map { |t| "-t #{t}" }.join(" ")
              dump_command = "pg_dump --verbose --schema-only --no-owner --no-acl --clean #{tables} #{to_url(source_uri)}"
              restore_command = "psql -q -d #{to_url(destination_uri)}"
              system("#{dump_command} | #{restore_command}")
            end

          log "* DONE (#{time.round(1)}s)"
        else
          from_uri = source_uri
          to_uri = destination_uri

          tables =
            if args[0] == "groups"
              specified_groups = to_arr(args[1])
              specified_groups.map do |group|
                if (tables = config["groups"][group])
                  tables
                else
                  abort "Group not found: #{group}"
                end
              end.flatten
            elsif args[0] == "tables"
              to_arr(args[1])
            elsif args[0]
              to_arr(args[0])
            else
              nil
            end

          with_connection(from_uri, timeout: 3) do |conn|
            tables ||= self.tables(conn, "public") - to_arr(opts[:exclude])

            tables.each do |table|
              unless table_exists?(conn, table, "public")
                abort "Table does not exist in source: #{table}"
              end
            end
          end

          with_connection(to_uri, timeout: 3) do |conn|
            tables.each do |table|
              unless table_exists?(conn, table, "public")
                abort "Table does not exist in destination: #{table}"
              end
            end
          end

          if opts[:list]
            if args[0] == "groups"
              pretty_list (config["groups"] || {}).keys
            else
              pretty_list tables
            end
          else
            in_parallel(tables) do |table|
              time =
                benchmark do
                  with_connection(from_uri) do |from_connection|
                    with_connection(to_uri) do |to_connection|
                      bad_fields = config["data_rules"]

                      from_fields = columns(from_connection, table, "public")
                      to_fields = columns(to_connection, table, "public")
                      shared_fields = to_fields & from_fields
                      extra_fields = to_fields - from_fields
                      missing_fields = from_fields - to_fields

                      from_sequences = sequences(from_connection, table, shared_fields)
                      to_sequences = sequences(to_connection, table, shared_fields)
                      shared_sequences = to_sequences & from_sequences
                      extra_sequences = to_sequences - from_sequences
                      missing_sequences = from_sequences - to_sequences

                      where = opts[:where]
                      limit = opts[:limit]
                      sql_clause = String.new

                      @mutex.synchronize do
                        log "* Syncing #{table}"
                        if where
                          log "    #{where}"
                          sql_clause << " WHERE #{opts[:where]}"
                        end
                        if limit
                          log "    LIMIT #{limit}"
                          sql_clause << " LIMIT #{limit}"
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
                        copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, bk| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], f, from_connection)} AS #{escape_identifier(f)}" : escape_identifier(f) }.join(", ")
                        fields = shared_fields.map { |f| escape_identifier(f) }.join(", ")

                        seq_values = {}
                        shared_sequences.each do |seq|
                          seq_values[seq] = from_connection.exec("select last_value from #{seq}").to_a[0]["last_value"]
                        end

                        to_connection.exec("TRUNCATE #{table} CASCADE")
                        to_connection.copy_data "COPY #{table} (#{fields}) FROM STDIN" do
                          from_connection.copy_data "COPY (SELECT #{copy_fields} FROM #{table}#{sql_clause}) TO STDOUT" do
                            while row = from_connection.get_copy_data
                              to_connection.put_copy_data(row)
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

            time = Time.now - start_time
            log "Completed in #{time.round(1)}s"
          end
        end
      end
      true
    end

    protected

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = %{Usage:
    pgsync [command] [options]

Commands:
    tables
    groups
    schema
    setup

Options:}
        o.string "--from", "source"
        o.string "--to", "destination"
        o.string "--where", "where"
        o.integer "--limit", "limit"
        o.string "--exclude", "exclude tables"
        o.string "--config", "config file"
        o.string "--db", "database"
        # TODO much better name for this option
        o.boolean "--to-safe", "accept danger", default: false
        o.boolean "--debug", "debug", default: false
        o.boolean "--list", "list", default: false
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

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table}.#{column}")
    end

    # TODO wildcard rules
    def apply_strategy(rule, column, conn)
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
          "unique_email" => "'email' || id || '@example.org'",
          "untouched" => escape_identifier(column),
          "unique_phone" => "(id + 1000000000)::text",
          "random_int" => "(RAND() * 10)::int",
          "random_date" => "'1970-01-01'",
          "random_time" => "NOW()",
          "unique_secret" => "'secret' || id",
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
        value.to_s.split(",")
      end
    end

    def parse_uri(url)
      uri = URI.parse(url)
      uri.scheme ||= "postgres"
      uri.host ||= "localhost"
      uri.port ||= 5432
      uri.path = "/#{uri.path}" if uri.path && uri.path[0] != "/"
      uri
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
      if @options[:debug]
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
  end
end
