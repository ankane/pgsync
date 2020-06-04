module PgSync
  class Table
    attr_reader :data_source

    def initialize(data_source:, schema:, table:)
      @data_source = data_source
      @schema = schema
      @table = table
    end

    def full_name
      [@schema, @table].join(".")
    end

    def columns
      @data_source.columns(full_name)
    end

    def sequences(shared_fields)
      @data_source.sequences(full_name, shared_fields)
    end
  end
end
