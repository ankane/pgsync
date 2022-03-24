DROP SCHEMA IF EXISTS public CASCADE;
DROP SCHEMA IF EXISTS other CASCADE;

CREATE SCHEMA public;
CREATE SCHEMA other;

CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY,
  email TEXT,
  phone TEXT,
  token TEXT,
  attempts INT,
  created_on DATE,
  updated_at TIMESTAMP,
  ip TEXT,
  name TEXT,
  nonsense TEXT,
  untouchable TEXT,
  env_token TEXT,
  "column_with_punctuation?" BOOLEAN
);

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title TEXT
);

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  post_id INTEGER REFERENCES posts(id) DEFERRABLE
);

CREATE TABLE comments2 (
  id SERIAL PRIMARY KEY,
  post_id INTEGER REFERENCES posts(id)
);

CREATE TABLE books (
  id SERIAL,
  id2 SERIAL,
  title TEXT,
  PRIMARY KEY (id, id2)
);

CREATE TABLE authors (
  last_name TEXT
);

CREATE TABLE chapters (
  pages BIGINT
);

CREATE TABLE robots (
  id SERIAL PRIMARY KEY,
  name TEXT
);
CREATE OR REPLACE FUNCTION nope()
RETURNS trigger AS
$$
BEGIN
  RAISE EXCEPTION 'Nope!';
END;
$$
LANGUAGE plpgsql;
CREATE TRIGGER nope_trigger BEFORE INSERT OR UPDATE ON robots FOR EACH ROW EXECUTE PROCEDURE nope();

CREATE TABLE excluded (
  id SERIAL PRIMARY KEY
);

CREATE TABLE other.pets (
  id SERIAL PRIMARY KEY
);
