DROP TABLE IF EXISTS "Users";
CREATE TABLE "Users" (
  "Id" SERIAL PRIMARY KEY,
  email varchar(255),
  zip_code varchar(255)
);
INSERT INTO "Users" (email) VALUES ('hi@example.org');
