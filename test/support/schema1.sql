DROP TABLE IF EXISTS "Users";
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

DROP TABLE IF EXISTS posts CASCADE;
CREATE TABLE posts (
  id SERIAL PRIMARY KEY,
  title TEXT
);

DROP TABLE IF EXISTS comments;
CREATE TABLE comments (
  id SERIAL PRIMARY KEY,
  post_id INTEGER REFERENCES posts(id)
);

DROP TABLE IF EXISTS comments2;
CREATE TABLE comments2 (
  id SERIAL PRIMARY KEY,
  post_id INTEGER REFERENCES posts(id)
);

DROP TABLE IF EXISTS books;
CREATE TABLE books (
  id SERIAL,
  id2 SERIAL,
  title TEXT,
  PRIMARY KEY (id, id2)
);

DROP TABLE IF EXISTS authors;
CREATE TABLE authors (
  first_name TEXT
);

DROP TABLE IF EXISTS chapters;
CREATE TABLE chapters (
  pages INT
);

DROP TABLE IF EXISTS robots;
CREATE TABLE robots (
  id SERIAL PRIMARY KEY,
  name TEXT
);

DROP SCHEMA IF EXISTS other CASCADE;
CREATE SCHEMA other;
CREATE TABLE other.pets (
  id SERIAL PRIMARY KEY
);
