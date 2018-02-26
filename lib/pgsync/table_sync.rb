module PgSync
  class TableSync
    def sync(mutex, config, table, opts, source_url, destination_url)
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

        mutex.synchronize do
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
            raise PgSync::Error, "Cannot use --overwrite with --in-batches" if opts[:overwrite]

            primary_key = source.primary_key(table)
            raise PgSync::Error, "No primary key" unless primary_key

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
            raise PgSync::Error, "No primary key" unless primary_key

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

    private

    def log(message = nil)
      $stderr.puts message
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
