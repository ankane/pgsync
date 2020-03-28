module PgSync
  class Init
    include Utils

    def perform(opts)
      @options = opts.to_hash
      config_file = db_config_file(opts.arguments[0]) || self.send(:config_file) || ".pgsync.yml"

      if File.exist?(config_file)
        raise Error, "#{config_file} exists."
      else
        contents = File.read(__dir__ + "/../../config.yml")

        # TODO improve code when adding another app
        if rails_app?
          ["exclude:", "  - schema_migrations", "  - ar_internal_metadata"].each do |line|
            contents.sub!("# #{line}", line)
          end
        end
        File.write(config_file, contents)
        log "#{config_file} created. Add your database credentials."
      end
    end

    # TODO maybe check parent directories
    def rails_app?
      File.exist?("bin/rails")
    end
  end
end
