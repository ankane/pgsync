DROP SCHEMA IF EXISTS public CASCADE;
DROP SCHEMA IF EXISTS other CASCADE;

CREATE SCHEMA public;
CREATE SCHEMA other;

DROP TYPE IF EXISTS mood;
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY,
  zip_code TEXT,
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
  "column_with_punctuation?" BOOLEAN,
  current_mood mood
);

CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title TEXT
);

CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  post_id INTEGER REFERENCES posts(id)
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
  first_name TEXT
);

CREATE TABLE chapters (
  pages INT
);

CREATE TABLE stores (
  name TEXT
);

CREATE TABLE robots (
  id SERIAL PRIMARY KEY,
  name TEXT
);

CREATE TABLE excluded (
  id SERIAL PRIMARY KEY
);

CREATE TABLE other.pets (
  id SERIAL PRIMARY KEY
);

INSERT INTO other.pets
VALUES
  (1),
  (2),
  (3);
