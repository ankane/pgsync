module PgSync
  class Task
    include Utils

    attr_reader :source, :destination, :config, :table, :opts
    attr_accessor :from_columns, :to_columns, :from_sequences, :to_sequences, :to_primary_key

    def initialize(source:, destination:, config:, table:, opts:)
      @source = source
      @destination = destination
      @config = config
      @table = table
      @opts = opts
      @from_sequences = []
      @to_sequences = []
    end

    def quoted_table
      quote_ident_full(table)
    end

    def field_equality(column)
      null_cond = "(#{quoted_table}.#{quote_ident(column)} IS NULL AND EXCLUDED.#{quote_ident(column)} IS NULL)"
      if to_types[column] == 'json'
        "(COALESCE(#{quoted_table}.#{quote_ident(column)}::jsonb = EXCLUDED.#{quote_ident(column)}::jsonb, false) OR #{null_cond})"
      else
        "(COALESCE(#{quoted_table}.#{quote_ident(column)} = EXCLUDED.#{quote_ident(column)}, false) OR #{null_cond})"
      end
    end

    def perform
      with_notices do
        handle_errors do
          maybe_disable_triggers do
            sync_data
          end
        end
      end
    end

    def from_fields
      @from_fields ||= from_columns.map { |c| c[:name] }
    end

    def to_fields
      @to_fields ||= to_columns.map { |c| c[:name] }
    end

    def from_types
      @from_types ||= from_columns.map { |c| [c[:name], c[:type]] }.to_h
    end

    def to_types
      @to_types ||= to_columns.map { |c| [c[:name], c[:type]] }.to_h
    end

    def shared_fields
      @shared_fields ||= to_fields & from_fields
    end

    def shared_sequences
      @shared_sequences ||= to_sequences & from_sequences
    end

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

    def prep_table
      if opts[:delete]
        destination.transaction do
          destination.execute("delete from #{quoted_table}")
          yield
        end
      # use delete instead of truncate for foreign keys
      elsif opts[:defer_constraints] || opts[:defer_constraints_v2]
        destination.execute("DELETE FROM #{quoted_table}")
        yield
      else
        destination.truncate(table)
        yield
      end
    end

    def sync_data
      raise Error, "This should never happen. Please file a bug." if shared_fields.empty?

      sql_clause = String.new("")
      sql_clause << " #{opts[:sql]}" if opts[:sql]
      if opts[:incremental] && !from_types['updated_at'].nil? && from_types['updated_at'].start_with?('timestamp')
        if !sql_clause.empty?
          sql_clause << " AND "
        else
          sql_clause << " WHERE "
        end
        sql_clause << "#{quoted_table}.updated_at > NOW() - INTERVAL '#{opts[:incremental]} second'"
      end
      bad_fields = opts[:no_rules] ? [] : config["data_rules"]
      primary_key = to_primary_key
      copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, _| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f, primary_key)} AS #{quote_ident(f)}" : "#{quoted_table}.#{quote_ident(f)}" }.join(", ")
      fields = shared_fields.map { |f| quote_ident(f) }.join(", ")

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
      elsif !(opts[:truncate] || opts[:delete]) && (opts[:overwrite] || opts[:preserve] || !sql_clause.empty?)
        raise Error, "No primary key" if primary_key.empty?

        destination.transaction do
          # creating many temp tables churns pg_attribute and pg_type
          # adding bloat and slowing down the DB
          if opts[:no_temp_table]
            temp_table = "pgsync_#{table}"
            destination.execute("CREATE TABLE IF NOT EXISTS #{quote_ident_full(temp_table)} (LIKE #{quoted_table} INCLUDING ALL)")
          # create a temp table
          else
            temp_table = "pgsync_#{rand(1_000_000_000)}"
            destination.execute("CREATE TEMPORARY TABLE #{quote_ident_full(temp_table)} ON COMMIT DROP AS TABLE #{quoted_table} WITH NO DATA")
          end

          # load data
          copy(copy_to_command, dest_table: temp_table, dest_fields: fields)

          on_conflict = primary_key.map { |pk| quote_ident(pk) }.join(", ")
          action =
            if opts[:preserve]
              "NOTHING"
            else # overwrite or sql clause
              setter = shared_fields.reject { |f| primary_key.include?(f) }.map { |f| "#{quote_ident(f)} = EXCLUDED.#{quote_ident(f)}" }
              if setter.any?
                "UPDATE SET #{setter.join(", ")} WHERE NOT (#{shared_fields.reject { |f| primary_key.include?(f) }.map { |f| field_equality(f) }.join(" AND ")})"
              else
                "NOTHING"
              end
            end
          result = destination.execute("INSERT INTO #{quoted_table} (#{fields}) (SELECT #{fields} FROM #{quote_ident_full(temp_table)}) ON CONFLICT (#{on_conflict}) DO #{action} returning *")

          # Protected by MVCC
          if opts[:no_temp_table]
            destination.execute("DELETE FROM #{quote_ident_full(temp_table)}")
          end
          log ({ table: table,  updated_rows: result.length })
        end
      else
        prep_table do
          copy(copy_to_command, dest_table: table, dest_fields: fields)
        end
      end

      # update sequences
      shared_sequences.each do |seq|
        value = source.last_value(seq)
        destination.execute("SELECT setval(#{escape(quote_ident_full(seq))}, #{escape(value)})")
      end

      {status: "success"}
    end

    private

    def with_notices
      notices = []
      [source, destination].each do |data_source|
        data_source.send(:conn).set_notice_processor do |message|
          notices << message.strip
        end
      end
      result = yield
      result[:notices] = notices if result
      result
    ensure
      # clear notice processor
      [source, destination].each do |data_source|
        data_source.send(:conn).set_notice_processor
      end
    end

    # TODO add retries
    def handle_errors
      yield
    rescue => e
      raise e if opts[:debug]

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

      source.log_sql(source_command)
      destination.log_sql(destination_command)
      bytes_count = 0
      start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)

      destination.conn.copy_data(destination_command) do
        source.conn.copy_data(source_command) do
          while (row = source.conn.get_copy_data)
            while opts[:throttle_bytes_per_second] && (bytes_count / (Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time)) > opts[:throttle_bytes_per_second]
              sleep(0.01)
            end
            destination.conn.put_copy_data(row)
            bytes_count += row.bytesize
          end
        end
      end
    end

    # TODO better performance
    def rule_match?(table, column, rule)
      regex = Regexp.new('\A' + Regexp.escape(rule).gsub('\*','[^\.]*') + '\z')
      regex.match(column) || regex.match("#{table.name}.#{column}") || regex.match("#{table.schema}.#{table.name}.#{column}")
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

    def maybe_disable_triggers
      if opts[:disable_integrity] || opts[:disable_integrity_v2] || opts[:disable_user_triggers]
        destination.transaction do
          triggers = destination.triggers(table)
          triggers.select! { |t| t["enabled"] == "t" }
          internal_triggers, user_triggers = triggers.partition { |t| t["internal"] == "t" }
          integrity_triggers = internal_triggers.select { |t| t["integrity"] == "t" }
          restore_triggers = []

          # both --disable-integrity options require superuser privileges
          # however, only v2 works on Amazon RDS, which added specific support for it
          # https://aws.amazon.com/about-aws/whats-new/2014/11/10/amazon-rds-postgresql-read-replicas/
          #
          # session_replication_role disables more than foreign keys (like triggers and rules)
          # this is probably fine, but keep the current default for now
          if opts[:disable_integrity_v2] || (opts[:disable_integrity] && rds?)
            # SET LOCAL lasts until the end of the transaction
            # https://www.postgresql.org/docs/current/sql-set.html
            destination.execute("SET LOCAL session_replication_role = replica")
          elsif opts[:disable_integrity]
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

    def rds?
      destination.execute("SELECT name, setting FROM pg_settings WHERE name LIKE 'rds.%'").any?
    end
  end
end
