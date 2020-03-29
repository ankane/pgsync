module PgSync
  class Init
    include Utils

    def perform(opts)
      # needed for config_file method
      @options = opts.to_hash

      file = db_config_file(opts.arguments[0]) || config_file || ".pgsync.yml"

      if File.exist?(file)
        raise Error, "#{file} exists."
      else
        exclude =
          if rails?
            <<~EOS
              exclude:
                - schema_migrations
                - ar_internal_metadata
            EOS
          else
            <<~EOS
              # exclude:
              #   - table1
              #   - table2
            EOS
          end

        # create file
        contents = File.read(__dir__ + "/../../config.yml")
        contents.sub!("$(some_command)", "$(heroku config:get DATABASE_URL)") if heroku?
        File.write(file, contents % {exclude: exclude})

        log "#{file} created. Add your database credentials."
      end
    end

    def heroku?
      `git remote -v`.include?("git.heroku.com") rescue false
    end

    def rails?
      File.exist?("bin/rails")
    end
  end
end
