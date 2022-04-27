Hi,

Please use this script to show what you're trying to do when possible. Thanks!

```sh
createdb pgsync_repro1
createdb pgsync_repro2

psql pgsync_repro1 << SQL
CREATE TABLE posts (
  id BIGINT PRIMARY KEY,
  name TEXT
);
INSERT INTO posts VALUES (1, 'hello'), (2, 'world');
SQL

psql pgsync_repro2 << SQL
CREATE TABLE posts (
  id BIGINT PRIMARY KEY,
  name TEXT
);
SQL

pgsync --from pgsync_repro1 --to pgsync_repro2
```
