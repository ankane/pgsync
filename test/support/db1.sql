DROP TABLE IF EXISTS users;
CREATE TABLE users (
  id integer NOT NULL,
  zip_code varchar(255),
  PRIMARY KEY (id)
);

CREATE SCHEMA sample_schema;

SET search_path=sample_schema;

DROP TABLE IF EXISTS users_from_schema;
CREATE TABLE users_from_schema (
  id integer NOT NULL,
  zip_code varchar(255),
  PRIMARY KEY (id)
);

SET search_path=public;
