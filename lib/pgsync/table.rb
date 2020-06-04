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
  end
end
