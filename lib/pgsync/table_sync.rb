module PgSync
  class TableSync
    include Utils

    attr_reader :source, :destination, :config, :table, :opts
    attr_accessor :from_columns, :to_columns

    def initialize(source:, destination:, config:, table:, opts:)
      @source = source
      @destination = destination
      @config = config
      @table = table
      @opts = opts
    end

    def quoted_table
      quote_ident_full(table)
    end

    def sync
      handle_errors do
        maybe_disable_triggers do
          sync_data
        end
      end
    end

    def from_fields
      @from_fields ||= from_columns.map { |c| c[:name] }
    end

    def to_fields
      @to_fields ||= to_columns.map { |c| c[:name] }
    end

    def shared_fields
      @shared_fields ||= to_fields & from_fields
    end

    def from_sequences
      @from_sequences ||= opts[:no_sequences] ? [] : source.sequences(table, shared_fields)
    end

    def to_sequences
      @to_sequences ||= opts[:no_sequences] ? [] : destination.sequences(table, shared_fields)
    end

    def shared_sequences
      @shared_sequences ||= to_sequences & from_sequences
    end

    # TODO add note when non-deferrable constraints on the table with --defer-constraints option
    def notes
      notes = []
      if shared_fields.empty?
        notes << "No fields to copy"
      else
        extra_fields = to_fields - from_fields
        notes << "Extra columns: #{extra_fields.join(", ")}" if extra_fields.any?

        missing_fields = from_fields - to_fields
        notes << "Missing columns: #{missing_fields.join(", ")}" if missing_fields.any?

        extra_sequences = to_sequences - from_sequences
        notes << "Extra sequences: #{extra_sequences.join(", ")}" if extra_sequences.any?

        missing_sequences = from_sequences - to_sequences
        notes << "Missing sequences: #{missing_sequences.join(", ")}" if missing_sequences.any?

        from_types = from_columns.map { |c| [c[:name], c[:type]] }.to_h
        to_types = to_columns.map { |c| [c[:name], c[:type]] }.to_h
        different_types = []
        shared_fields.each do |field|
          if from_types[field] != to_types[field]
            different_types << "#{field} (#{from_types[field]} -> #{to_types[field]})"
          end
        end
        notes << "Different column types: #{different_types.join(", ")}" if different_types.any?
      end
      notes
    end

    def sync_data
      raise Error, "This should never happen. Please file a bug." if shared_fields.empty?

      start_time = Time.now

      sql_clause = String.new("")
      sql_clause << " #{opts[:sql]}" if opts[:sql]

      bad_fields = opts[:no_rules] ? [] : config["data_rules"]
      primary_key = destination.primary_key(table)
      copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, _| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f, primary_key)} AS #{quote_ident(f)}" : "#{quoted_table}.#{quote_ident(f)}" }.join(", ")
      fields = shared_fields.map { |f| quote_ident(f) }.join(", ")

      seq_values = {}
      shared_sequences.each do |seq|
        seq_values[seq] = source.last_value(seq)
      end

      copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quoted_table}#{sql_clause}) TO STDOUT"
      if opts[:in_batches]
        raise Error, "No primary key" if primary_key.empty?
        primary_key = primary_key.first

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

          batch_copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quoted_table}#{batch_sql_clause}) TO STDOUT"
          copy(batch_copy_to_command, dest_table: table, dest_fields: fields)

          starting_id += batch_size
          i += 1

          if opts[:sleep] && starting_id <= from_max_id
            sleep(opts[:sleep])
          end
        end
      elsif !opts[:truncate] && (opts[:overwrite] || opts[:preserve] || !sql_clause.empty?)
        raise Error, "No primary key" if primary_key.empty?

        # TODO handle multiple primary keys
        primary_key = primary_key.first

        # create a temp table
        temp_table = "pgsync_#{rand(1_000_000_000)}"
        destination.execute("CREATE TEMPORARY TABLE #{quote_ident_full(temp_table)} AS TABLE #{quoted_table} WITH NO DATA")

        # load data
        copy(copy_to_command, dest_table: temp_table, dest_fields: fields)

        if opts[:update]

        elsif opts[:preserve]
          # insert into
          destination.execute("INSERT INTO #{quoted_table} (SELECT * FROM #{quote_ident_full(temp_table)} WHERE NOT EXISTS (SELECT 1 FROM #{quoted_table} WHERE #{quoted_table}.#{quote_ident(primary_key)} = #{quote_ident_full(temp_table)}.#{quote_ident(primary_key)}))")
        else
          setter = shared_fields
            .filter { |field| not field.eql? primary_key }
            .map { |f| "#{quote_ident(f)} = EXCLUDED.#{quote_ident(f)}" }.join ", "
          destination.execute "INSERT INTO #{quote_ident_full(table)} (SELECT * FROM #{quote_ident(temp_table)}) ON CONFLICT (#{primary_key}) DO UPDATE SET #{setter}"
        end
      else
        # use delete instead of truncate for foreign keys
        if opts[:defer_constraints]
          destination.execute("DELETE FROM #{quoted_table}")
        else
          destination.truncate(table)
        end
        copy(copy_to_command, dest_table: table, dest_fields: fields)
      end
      seq_values.each do |seq, value|
        destination.execute("SELECT setval(#{escape(seq)}, #{escape(value)})")
      end

      {status: "success", time: (Time.now - start_time).round(1)}
    end

    private

    # TODO add retries
    def handle_errors
      yield
    rescue => e
      message =
        case e
        when PG::ConnectionBad
          # likely fine to show simplified message here
          # the full message will be shown when first trying to connect
          "Connection failed"
        when PG::Error
          e.message.sub("ERROR:  ", "")
        when Error
          e.message
        else
          "#{e.class.name}: #{e.message}"
        end

      {status: "error", message: message}
    end

    def copy(source_command, dest_table:, dest_fields:)
      destination_command = "COPY #{quote_ident_full(dest_table)} (#{dest_fields}) FROM STDIN"
      destination.conn.copy_data(destination_command) do
        source.conn.copy_data(source_command) do
          while (row = source.conn.get_copy_data)
            destination.conn.put_copy_data(row)
          end
        end
      end
    end

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table.split(".", 2)[-1]}.#{column}") || regex.match("#{table}.#{column}")
    end

    # TODO wildcard rules
    def apply_strategy(rule, table, column, primary_key)
      if rule.is_a?(Hash)
        if rule.key?("value")
          escape(rule["value"])
        elsif rule.key?("statement")
          rule["statement"]
        else
          raise Error, "Unknown rule #{rule.inspect} for column #{column}"
        end
      else
        case rule
        when "untouched"
          quote_ident(column)
        when "unique_email"
          "'email' || #{quoted_primary_key(table, primary_key, rule)}::text || '@example.org'"
        when "unique_phone"
          "(#{quoted_primary_key(table, primary_key, rule)}::bigint + 1000000000)::text"
        when "unique_secret"
          "'secret' || #{quoted_primary_key(table, primary_key, rule)}::text"
        when "random_int", "random_number"
          "(RANDOM() * 100)::int"
        when "random_date"
          "date '1970-01-01' + (RANDOM() * 10000)::int"
        when "random_time"
          "NOW() - (RANDOM() * 100000000)::int * INTERVAL '1 second'"
        when "random_ip"
          "(1 + RANDOM() * 254)::int::text || '.0.0.1'"
        when "random_letter"
          "chr(65 + (RANDOM() * 26)::int)"
        when "random_string"
          "RIGHT(MD5(RANDOM()::text), 10)"
        when "null", nil
          "NULL"
        else
          raise Error, "Unknown rule #{rule} for column #{column}"
        end
      end
    end

    def quoted_primary_key(table, primary_key, rule)
      raise Error, "Single column primary key required for this data rule: #{rule}" unless primary_key.size == 1
      "#{quoted_table}.#{quote_ident(primary_key.first)}"
    end

    def quote_ident_full(ident)
      ident.split(".").map { |v| quote_ident(v) }.join(".")
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

    def maybe_disable_triggers
      if opts[:disable_integrity] || opts[:disable_user_triggers]
        destination.transaction do
          triggers = destination.triggers(table)
          triggers.select! { |t| t["enabled"] == "t" }
          internal_triggers, user_triggers = triggers.partition { |t| t["internal"] == "t" }
          integrity_triggers = internal_triggers.select { |t| t["integrity"] == "t" }
          restore_triggers = []

          if opts[:disable_integrity]
            integrity_triggers.each do |trigger|
              destination.execute("ALTER TABLE #{quoted_table} DISABLE TRIGGER #{quote_ident(trigger["name"])}")
            end
            restore_triggers.concat(integrity_triggers)
          end

          if opts[:disable_user_triggers]
            # important!
            # rely on Postgres to disable user triggers
            # we don't want to accidentally disable non-user triggers if logic above is off
            destination.execute("ALTER TABLE #{quoted_table} DISABLE TRIGGER USER")
            restore_triggers.concat(user_triggers)
          end

          result = yield

          # restore triggers that were previously enabled
          restore_triggers.each do |trigger|
            destination.execute("ALTER TABLE #{quoted_table} ENABLE TRIGGER #{quote_ident(trigger["name"])}")
          end

          result
        end
      else
        yield
      end
    end
  end
end
