require "pgsync/version"
require "yaml"
require "slop"
require "uri"
require "erb"
require "pg"
require "parallel"
require "multiprocessing"
require "fileutils"

module PgSync
  class Error < StandardError; end
  class Rollback < StandardError; end

  class Client
    def initialize(args)
      $stdout.sync = true
      @arguments, @options = parse_args(args)
      @config_file = @options[:config]
      @mutex = MultiProcessing::Mutex.new
    end

    # TODO clean up this mess
    def perform
      start_time = Time.now

      args, opts = @arguments, @options
      [:to, :from, :to_safe, :exclude].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end
      command = args[0]

      if command == "setup"
        setup
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
              puts "* Dumping schema"
              tables = to_arr(args[1]).map { |t| "-t #{t}" }.join(" ")
              dump_command = "pg_dump --verbose --schema-only --no-owner --no-acl --clean #{tables} #{to_url(source_uri)}"
              restore_command = "psql -q -d #{to_url(destination_uri)}"
              system("#{dump_command} | #{restore_command}")
            end

          puts "* DONE (#{time.round(1)}s)"
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

          Parallel.each(tables) do |table|
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

                    @mutex.synchronize do
                      puts "* Syncing #{table}"
                      if where
                        puts "    #{where}"
                        where = " WHERE #{opts[:where]}"
                      end
                      puts "EXTRA COLUMNS: #{extra_fields}" if extra_fields.any?
                      puts "MISSING COLUMNS: #{missing_fields}" if missing_fields.any?
                      puts "EXTRA SEQUENCES: #{extra_sequences}" if extra_sequences.any?
                      puts "MISSING SEQUENCES: #{missing_sequences}" if missing_sequences.any?
                    end

                    if shared_fields.empty?
                      abort "No fields to copy: #{table}"
                    end

                    copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, bk| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], f, from_connection)} AS #{escape_identifier(f)}" : escape_identifier(f) }.join(", ")
                    fields = shared_fields.map { |f| escape_identifier(f) }.join(", ")

                    seq_values = {}
                    shared_sequences.each do |seq|
                      seq_values[seq] = from_connection.exec("select last_value from #{seq}").to_a[0]["last_value"]
                    end

                    # use transaction to revert statement timeout
                    begin
                      from_connection.transaction do |fconn|
                        fconn.exec("SET statement_timeout = 0")
                        to_connection.exec("TRUNCATE #{table} CASCADE")
                        to_connection.copy_data "COPY #{table} (#{fields}) FROM STDIN" do
                          fconn.copy_data "COPY (SELECT #{copy_fields} FROM #{table}#{where}) TO STDOUT" do
                            while row = fconn.get_copy_data
                              to_connection.put_copy_data(row)
                            end
                          end
                        end
                        seq_values.each do |seq, value|
                          to_connection.exec("SELECT setval(#{escape(seq)}, #{escape(value)})")
                        end
                        raise PgSync::Rollback
                      end
                    rescue PgSync::Rollback
                      # success
                    end
                  end
                end
              end

            @mutex.synchronize do
              puts "* DONE #{table} (#{time.round(1)}s)"
            end
          end

          time = Time.now - start_time
          puts "Completed in #{time.round(1)}s"
        end
      end
      true
    end

    protected

    def parse_args(args)
      opts = Slop.parse(args) do |o|
        o.banner = "usage: pgsync [options]"
        o.string "--from", "source"
        o.string "--to", "destination"
        o.string "--where", "where"
        o.string "--exclude", "exclude tables"
        o.string "--config", "config file", default: ".pgsync.yml"
        # TODO much better name for this option
        o.boolean "--to-safe", "accept danger", default: false
        o.on "-v", "--version", "print the version" do
          puts PgSync::VERSION
          exit
        end
        o.on "-h", "--help", "prints help" do
          puts o
          exit
        end
      end
      [opts.arguments, opts.to_hash]
    rescue Slop::Error => e
      abort e.message
    end

    # TODO look down path
    def config
      @config ||= begin
        if File.exist?(@config_file)
          begin
            YAML.load_file(@config_file) || {}
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
        source = `#{source[2..-2]}`.chomp
      end
      source
    end

    def setup
      if File.exist?(@config_file)
        abort "#{@config_file} exists."
      else
        FileUtils.cp(File.dirname(__FILE__) + "/../config.yml", @config_file)
        puts "#{@config_file} created. Add your database credentials."
      end
    end

    def with_connection(uri, timeout: 0)
      conn =
        PG::Connection.new(
          host: uri.host,
          port: uri.port,
          dbname: uri.path.sub(/\A\//, ""),
          user: uri.user,
          password: uri.password,
          connect_timeout: timeout
        )
      begin
        yield conn
      ensure
        conn.close
      end
    rescue PG::ConnectionBad => e
      puts
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
      uri.host ||= "localhost"
      uri.port ||= 5432
      uri
    end

    def print_uri(prefix, uri)
      puts "#{prefix}: #{uri.path.sub(/\A\//, '')} on #{uri.host}:#{uri.port}"
    end

    def to_url(uri)
      uri = uri.dup
      uri.query = nil
      uri.to_s
    end

    def abort(message)
      raise PgSync::Error, message
    end

    def sequences(conn, table, columns)
      conn.exec("SELECT #{columns.map { |f| "pg_get_serial_sequence(#{escape(table)}, #{escape(f)}) AS #{f}" }.join(", ")}").to_a[0].values.compact
    end
  end
end
