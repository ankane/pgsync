module PgSync
  class Sync
    include Utils

    def initialize(arguments, options)
      @arguments = arguments
      @options = options
    end

    def perform
      started_at = Time.now

      args = @arguments
      opts = @options

      # only resolve commands from config, not CLI arguments
      [:to, :from].each do |opt|
        opts[opt] ||= resolve_source(config[opt.to_s])
      end

      # merge other config
      [:to_safe, :exclude, :schemas].each do |opt|
        opts[opt] ||= config[opt.to_s]
      end

      if args.size > 2
        raise Error, "Usage:\n    pgsync [options]"
      end

      raise Error, "No source" unless source.exists?
      raise Error, "No destination" unless destination.exists?

      unless opts[:to_safe] || destination.local?
        raise Error, "Danger! Add `to_safe: true` to `.pgsync.yml` if the destination is not localhost or 127.0.0.1"
      end

      if (opts[:preserve] || opts[:overwrite]) && destination.server_version_num < 90500
        raise Error, "Postgres 9.5+ is required for --preserve and --overwrite"
      end

      print_description("From", source)
      print_description("To", destination)

      resolver = TaskResolver.new(args: args, opts: opts, source: source, destination: destination, config: config, first_schema: first_schema)
      tasks =
        resolver.tasks.map do |task|
          Task.new(source: source, destination: destination, config: config, table: task[:table], opts: opts.merge(sql: task[:sql]))
        end

      if opts[:in_batches] && tasks.size > 1
        raise Error, "Cannot use --in-batches with multiple tables"
      end

      confirm_tables_exist(source, tasks, "source")

      if opts[:list]
        confirm_tables_exist(destination, tasks, "destination")
        tasks.each do |task|
          log task_name(task)
        end
      else
        if opts[:schema_first] || opts[:schema_only]
          SchemaSync.new(source: source, destination: destination, tasks: tasks, args: args, opts: opts).perform
        end

        unless opts[:schema_only]
          TableSync.new(source: source, destination: destination, tasks: tasks, opts: opts, resolver: resolver).perform
        end

        log_completed(started_at)
      end
    end

    private

    def config
      @config ||= begin
        file = config_file
        if file
          begin
            YAML.load_file(file) || {}
          rescue Psych::SyntaxError => e
            raise Error, e.message
          rescue Errno::ENOENT
            raise Error, "Config file not found: #{file}"
          end
        else
          {}
        end
      end
    end

    def config_file
      if @options[:config]
        @options[:config]
      elsif @options[:db]
        file = db_config_file(@options[:db])
        search_tree(file) || file
      else
        search_tree(".pgsync.yml")
      end
    end

    def search_tree(file)
      return file if File.exist?(file)

      path = Dir.pwd
      # prevent infinite loop
      20.times do
        absolute_file = File.join(path, file)
        break absolute_file if File.exist?(absolute_file)
        path = File.dirname(path)
        break if path == "/"
      end
    end

    def print_description(prefix, source)
      location = " on #{source.host}:#{source.port}" if source.host
      log "#{prefix}: #{source.dbname}#{location}"
    end

    def log_completed(started_at)
      time = Time.now - started_at
      message = "Completed in #{time.round(1)}s"
      log colorize(message, :green)
    end

    def source
      @source ||= data_source(@options[:from])
    end

    def destination
      @destination ||= data_source(@options[:to])
    end

    def data_source(url)
      ds = DataSource.new(url)
      ObjectSpace.define_finalizer(self, self.class.finalize(ds))
      ds
    end

    # ideally aliases would work, but haven't found a nice way to do this
    def resolve_source(source)
      if source
        source = source.dup
        source.gsub!(/\$\([^)]+\)/) do |m|
          command = m[2..-2]
          result = `#{command}`.chomp
          unless $?.success?
            raise Error, "Command exited with non-zero status:\n#{command}"
          end
          result
        end
      end
      source
    end

    def self.finalize(ds)
      # must use proc instead of stabby lambda
      proc { ds.close }
    end
  end
end
