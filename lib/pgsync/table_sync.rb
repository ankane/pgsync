module PgSync
  class TableSync
    def sync(mutex, config, table, opts, source_url, destination_url, first_schema)
      start_time = Time.now
      source = DataSource.new(source_url, timeout: 0)
      destination = DataSource.new(destination_url, timeout: 0)

      begin
        from_connection = source.conn
        to_connection = destination.conn

        bad_fields = opts[:no_rules] ? [] : config["data_rules"]

        from_fields, from_fields_types = source.columns(table)
        to_fields, to_fields_types = destination.columns(table)

        shared_fields = to_fields & from_fields
        extra_fields = to_fields - from_fields
        missing_fields = from_fields - to_fields

        from_sequences = source.sequences(table, shared_fields)
        to_sequences = destination.sequences(table, shared_fields)
        shared_sequences = to_sequences & from_sequences
        extra_sequences = to_sequences - from_sequences
        missing_sequences = from_sequences - to_sequences

        sql_clause = String.new

        table_name = table.sub("#{first_schema}.", "")

        mutex.synchronize do
          log "* Syncing #{table_name}"
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
          primary_key = destination.primary_key(table)
          copy_fields = shared_fields.map { |f| f2 = bad_fields.to_a.find { |bf, _| rule_match?(table, f, bf) }; f2 ? "#{apply_strategy(f2[1], table, f, primary_key)} AS #{quote_ident(f)}" : "#{quote_ident_full(table)}.#{quote_ident(f)}" }.join(", ")
          fields = shared_fields.map { |f| quote_ident(f) }.join(", ")

          seq_values = {}
          shared_sequences.each do |seq|
            seq_values[seq] = source.last_value(seq)
          end

          copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident_full(table)}#{sql_clause}) TO STDOUT"
          if opts[:in_batches]
            raise PgSync::Error, "Cannot use --overwrite with --in-batches" if opts[:overwrite]
            raise PgSync::Error, "No primary key" unless primary_key

            destination.truncate(table) if opts[:truncate]

            if from_fields_types[primary_key] == "integer"
              # INTEGER BATCHING (original)
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

                batch_copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident_full(table)}#{batch_sql_clause}) TO STDOUT"
                to_connection.copy_data "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN" do
                  from_connection.copy_data batch_copy_to_command do
                    while (row = from_connection.get_copy_data)
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
            elsif from_fields_types[primary_key] == "uuid"
              # UUID BATCHING (new)
              total_rows = source.row_count(table)
              batch_size = opts[:batch_size]

              modulo = (total_rows.to_f / batch_size.to_f).ceil

              current_batch_position = 0

              while current_batch_position < modulo
                # Convert UUID to BigInt in the where clause then take the absolute(modulo)
                where_modulo = "
                    @ (('x' || translate(left(#{quote_ident(primary_key)}::text, 18), '-', ''))::bit(64)::bigint)
                    % #{modulo} = #{current_batch_position}
                "

                log "    #{current_batch_position + 1}/#{modulo}: #{where}"

                batch_sql_clause = " #{sql_clause.length > 0 ? "#{sql_clause} AND" : "WHERE"} #{where_modulo}"

                batch_copy_to_command = "COPY (SELECT #{copy_fields} FROM #{quote_ident_full(table)}#{batch_sql_clause}) TO STDOUT"
                to_connection.copy_data "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN" do
                  from_connection.copy_data batch_copy_to_command do
                    while (row = from_connection.get_copy_data)
                      to_connection.put_copy_data(row)
                    end
                  end
                end

                current_batch_position += 1

                if opts[:sleep] && current_batch_position < modulo
                  sleep(opts[:sleep])
                end
              end
            else
              raise PgSync::Error, "Can only use --in-batches with columns of type 'integer' and 'uuid'"
            end
          elsif !opts[:truncate] && (opts[:overwrite] || opts[:preserve] || !sql_clause.empty?)
            raise PgSync::Error, "No primary key" unless primary_key

            temp_table = "pgsync_#{rand(1_000_000_000)}"
            file = Tempfile.new(temp_table)
            begin
              from_connection.copy_data copy_to_command do
                while (row = from_connection.get_copy_data)
                  file.write(row)
                end
              end
              file.rewind

              # create a temp table
              to_connection.exec("CREATE TEMPORARY TABLE #{quote_ident_full(temp_table)} AS SELECT * FROM #{quote_ident_full(table)} WITH NO DATA")

              # load file
              to_connection.copy_data "COPY #{quote_ident_full(temp_table)} (#{fields}) FROM STDIN" do
                file.each do |row|
                  to_connection.put_copy_data(row)
                end
              end

              if opts[:preserve]
                # insert into
                to_connection.exec("INSERT INTO #{quote_ident_full(table)} (SELECT * FROM #{quote_ident_full(temp_table)} WHERE NOT EXISTS (SELECT 1 FROM #{quote_ident_full(table)} WHERE #{quote_ident_full(table)}.#{quote_ident(primary_key)} = #{quote_ident_full(temp_table)}.#{quote_ident(primary_key)}))")
              else
                to_connection.transaction do
                  to_connection.exec("DELETE FROM #{quote_ident_full(table)} WHERE #{quote_ident(primary_key)} IN (SELECT #{quote_ident(primary_key)} FROM #{quote_ident_full(temp_table)})")
                  to_connection.exec("INSERT INTO #{quote_ident_full(table)} (SELECT * FROM #{quote_ident(temp_table)})")
                end
              end
            ensure
              file.close
              file.unlink
            end
          else
            destination.truncate(table)
            to_connection.copy_data "COPY #{quote_ident_full(table)} (#{fields}) FROM STDIN" do
              from_connection.copy_data copy_to_command do
                while (row = from_connection.get_copy_data)
                  to_connection.put_copy_data(row)
                end
              end
            end
          end
          seq_values.each do |seq, value|
            to_connection.exec("SELECT setval(#{escape(seq)}, #{escape(value)})")
          end
        end
        mutex.synchronize do
          log "* DONE #{table_name} (#{(Time.now - start_time).round(1)}s)"
        end
      ensure
        source.close
        destination.close
      end
    end

    private

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
          raise PgSync::Error, "Unknown rule #{rule.inspect} for column #{column}"
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
          raise PgSync::Error, "Unknown rule #{rule} for column #{column}"
        end
      end
    end

    def quoted_primary_key(table, primary_key, rule)
      raise "Primary key required for this data rule: #{rule}" unless primary_key
      "#{quote_ident_full(table)}.#{quote_ident(primary_key)}"
    end

    def log(message = nil)
      $stderr.puts message
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
  end
end

