module PgSync
  class Init
    include Utils

    def initialize(arguments, options)
      @arguments = arguments
      @options = options
    end

    def perform
      if @arguments.size > 1
        raise Error, "Usage:\n    pgsync --init [db]"
      end

      file =
        if @options[:config]
          @options[:config]
        elsif @arguments.any?
          db_config_file(@arguments.first)
        elsif @options[:db]
          db_config_file(@options[:db])
        else
          ".pgsync.yml"
        end

      if File.exist?(file)
        raise Error, "#{file} exists."
      else
        exclude =
          if rails?
            <<~EOS
              exclude:
                - ar_internal_metadata
                - schema_migrations
            EOS
          elsif django?
            # TODO exclude other tables?
            <<~EOS
              exclude:
                - django_migrations
            EOS
          elsif laravel?
            <<~EOS
              exclude:
                - migrations
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

    def django?
      (File.read("manage.py") =~ /django/i) rescue false
    end

    def heroku?
      `git remote -v 2>&1`.include?("git.heroku.com") rescue false
    end

    def laravel?
      File.exist?("artisan")
    end

    def rails?
      File.exist?("bin/rails")
    end
  end
end
