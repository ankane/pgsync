module PgSync
  class Init
    include Utils

    def perform(opts)
      @options = opts.to_hash
      config_file = db_config_file(opts.arguments[0]) || self.send(:config_file) || ".pgsync.yml"

      if File.exist?(config_file)
        raise Error, "#{config_file} exists."
      else
        exclude =
          if rails_app?
            <<~EOS
              exclude:
                - schema_migrations
                - ar_internal_metadata
            EOS
          else
            <<~EOS
              # exclude:
              #   - schema_migrations
              #   - ar_internal_metadata
            EOS
          end

        # create file
        contents = File.read(__dir__ + "/../../config.yml")
        File.write(config_file, contents % {exclude: exclude})

        log "#{config_file} created. Add your database credentials."
      end
    end

    # TODO maybe check parent directories
    def rails_app?
      File.exist?("bin/rails")
    end
  end
end
