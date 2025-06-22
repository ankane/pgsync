---
name: Issue
about: Create an issue
---

Hi,

Please use this script to show what you're trying to do when possible. Thanks!

```sh
createdb pgsync_from
createdb pgsync_to

psql pgsync_from << SQL
CREATE TABLE posts (
  id BIGINT PRIMARY KEY,
  name TEXT
);
INSERT INTO posts VALUES (1, 'hello'), (2, 'world');
SQL

psql pgsync_to << SQL
CREATE TABLE posts (
  id BIGINT PRIMARY KEY,
  name TEXT
);
SQL

pgsync --from pgsync_from --to pgsync_to
```
