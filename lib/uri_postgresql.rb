module URI
  class POSTGRESQL < Generic
    DEFAULT_PORT = 5432
  end
  @@schemes['POSTGRESQL'] = @@schemes['POSTGRES'] = POSTGRESQL
end
