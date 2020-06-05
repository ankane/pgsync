# minimal class to keep schema and table name separate
module PgSync
  class Table
    attr_reader :schema, :name

    def initialize(schema, name)
      @schema = schema
      @name = name
    end

    def full_name
      "#{schema}.#{name}"
    end

    def eql?(other)
      other.schema == schema && other.name == name
    end

    # override hash when overriding eql?
    def hash
      [schema, name].hash
    end

    def to_s
      full_name
    end
  end
end
