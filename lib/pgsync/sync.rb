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

      # TODO remove?
      if opts[:list]
        confirm_tables_exist(destination, tasks, "destination")
        pretty_list tasks.map { |task| task_name(task) }
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

    def print_description(prefix, source)
      location = " on #{source.host}:#{source.port}" if source.host
      log "#{prefix}: #{source.dbname}#{location}"
    end

    def pretty_list(items)
      items.each do |item|
        log item
      end
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
