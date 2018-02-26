module PgSync
  class Client
    def initialize(args)
      $stdout.sync = true
      @exit = false
      @arguments, @options = parse_args(args)
      @mutex = windows? ? Mutex.new : MultiProcessing::Mutex.new
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

        source = DataSource.new(opts[:from])
        abort "No source" unless source.exists?

        destination = DataSource.new(opts[:to])
        abort "No destination" unless destination.exists?

        unless opts[:to_safe] || destination.local?
          abort "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
        end

        print_description("From", source)
        print_description("To", destination)

        tables = nil
        begin
          tables = TableList.new(args, opts, source).tables
        ensure
          source.close
        end

        if opts[:schema_only]
          log "* Dumping schema"
          tables = tables.keys.map { |t| "-t #{Shellwords.escape(quote_ident(t))}" }.join(" ")
          psql_version = Gem::Version.new(`psql --version`.lines[0].chomp.split(" ")[-1].sub(/beta\d/, ""))
          if_exists = psql_version >= Gem::Version.new("9.4.0")
          dump_command = "pg_dump -Fc --verbose --schema-only --no-owner --no-acl #{tables} #{source.to_url}"
          restore_command = "pg_restore --verbose --no-owner --no-acl --clean #{if_exists ? "--if-exists" : nil} -d #{destination.to_url}"
          system("#{dump_command} | #{restore_command}")

          log_completed(start_time)
        else
          begin
            tables.keys.each do |table|
              unless destination.table_exists?(table)
                abort "Table does not exist in destination: #{table}"
              end
            end
          ensure
            destination.close
          end

          if opts[:list]
            if args[0] == "groups"
              pretty_list (config["groups"] || {}).keys
            else
              pretty_list tables.keys
            end
          else
            in_parallel(tables) do |table, table_opts|
              sync_table(table, opts.merge(table_opts), source.url, destination.url)
            end

            log_completed(start_time)
          end
        end
      end
      true
    end

    protected

    def sync_table(table, opts, source_url, destination_url)
      time =
        benchmark do
          source = DataSource.new(source_url)
          destination = DataSource.new(destination_url)

          from_connection = source.conn
          to_connection = destination.conn

          begin
            bad_fields = opts[:no_rules] ? [] : config["data_rules"]

            from_fields = source.columns(table)
            to_fields = destination.columns(table)
            shared_fields = to_fields & from_fields
            extra_fields = to_fields - from_fields
            missing_fields = from_fields - to_fields

            from_sequences = source.sequences(table, shared_fields)
            to_sequences = destination.sequences(table, shared_fields)
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
              copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, bk| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f)} AS #{quote_ident(f)}" : "#{quote_ident(table)}.#{quote_ident(f)}" }.join(", ")
              fields = shared_fields.map { |f| quote_ident(f) }.join(", ")

              seq_values = {}
              shared_sequences.each do |seq|
                seq_values[seq] = source.last_value(seq)
              end

              copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident(table)}#{sql_clause}) TO STDOUT"
              if opts[:in_batches]
                abort "Cannot use --overwrite with --in-batches" if opts[:overwrite]

                primary_key = source.primary_key(table)
                abort "No primary key" unless primary_key

                destination.truncate(table) if opts[:truncate]

                from_max_id = source.max_id(table, primary_key)
                to_max_id = destination.max_id(table, primary_key) + 1

                if to_max_id == 1
                  from_min_id = source.min_id(table, primary_key)
                  to_max_id = from_min_id if from_min_id > 0
                end

                starting_id = to_max_id
                batch_size = opts[:batch_size]

                i = 1
                batch_count = ((from_max_id - starting_id + 1) / batch_size.to_f).ceil

                while starting_id <= from_max_id
                  where = "#{quote_ident(primary_key)} >= #{starting_id} AND #{quote_ident(primary_key)} < #{starting_id + batch_size}"
                  log "    #{i}/#{batch_count}: #{where}"

                  # TODO be smarter for advance sql clauses
                  batch_sql_clause = " #{sql_clause.length > 0 ? "#{sql_clause} AND" : "WHERE"} #{where}"

                  batch_copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident(table)}#{batch_sql_clause}) TO STDOUT"
                  to_connection.copy_data "COPY #{quote_ident(table)} (#{fields}) FROM STDIN" do
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
                primary_key = destination.primary_key(table)
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
                    to_connection.exec("CREATE TABLE #{quote_ident(temp_table)} AS SELECT * FROM #{quote_ident(table)} WITH NO DATA")

                    # load file
                    to_connection.copy_data "COPY #{quote_ident(temp_table)} (#{fields}) FROM STDIN" do
                      file.each do |row|
                        to_connection.put_copy_data(row)
                      end
                    end

                    if opts[:preserve]
                      # insert into
                      to_connection.exec("INSERT INTO #{quote_ident(table)} (SELECT * FROM #{quote_ident(temp_table)} WHERE NOT EXISTS (SELECT 1 FROM #{quote_ident(table)} WHERE #{quote_ident(table)}.#{primary_key} = #{quote_ident(temp_table)}.#{quote_ident(primary_key)}))")
                    else
                      to_connection.exec("DELETE FROM #{quote_ident(table)} WHERE #{quote_ident(primary_key)} IN (SELECT #{quote_ident(primary_key)} FROM #{quote_ident(temp_table)})")
                      to_connection.exec("INSERT INTO #{quote_ident(table)} (SELECT * FROM #{quote_ident(temp_table)})")
                    end

                    # delete temp table
                    to_connection.exec("DROP TABLE #{quote_ident(temp_table)}")
                  end
                ensure
                   file.close
                   file.unlink
                end
              else
                destination.truncate(table)
                to_connection.copy_data "COPY #{quote_ident(table)} (#{fields}) FROM STDIN" do
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
          ensure
            source.close
            destination.close
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

    def benchmark
      start_time = Time.now
      yield
      Time.now - start_time
    end

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table}.#{column}")
    end

    # TODO wildcard rules
    def apply_strategy(rule, table, column)
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
          "untouched" => quote_ident(column),
          "unique_phone" => "(#{table}.id + 1000000000)::text",
          "random_int" => "(RAND() * 10)::int",
          "random_date" => "'1970-01-01'",
          "random_time" => "NOW()",
          "unique_secret" => "'secret' || #{table}.id",
          "random_ip" => "'127.0.0.1'",
          "random_letter" => "'A'",
          "random_string" => "right(md5(random()::text),10)",
          "random_number" => "(RANDOM() * 1000000)::int",
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

    def quote_ident(value)
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

    def print_description(prefix, source)
      log "#{prefix}: #{source.uri.path.sub(/\A\//, '')} on #{source.uri.host}:#{source.uri.port}"
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

    def in_parallel(tables, &block)
      if @options[:debug] || @options[:in_batches]
        tables.each(&block)
      else
        options = {}
        options[:in_threads] = 4 if windows?
        Parallel.each(tables, options, &block)
      end
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
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

    def windows?
      Gem.win_platform?
    end
  end
end
