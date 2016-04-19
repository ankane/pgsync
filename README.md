# pgsync

Sync Postgres data to your local machine. Designed for:

- **speed** - up to 4x faster than traditional tools on a 4-core machine
- **security** - built-in methods to prevent sensitive data from ever leaving the server
- **convenience** - sync partial tables, groups of tables, and related records

:tangerine: Battle-tested at [Instacart](https://www.instacart.com/opensource)

## Installation

```sh
gem install pgsync
```

And in your project directory, run:

```sh
pgsync --setup
```

This creates `.pgsync.yml` for you to customize. We recommend checking this into your version control (assuming it doesn’t contain sensitive information). `pgsync` commands can be run from this directory or any subdirectory.

## How to Use

Sync all tables

```sh
pgsync
```

Sync specific tables

```sh
pgsync table1,table2
```

Sync specific rows (existing rows are overwritten)

```sh
pgsync products "where store_id = 1"
```

You can also preserve existing rows

```sh
pgsync products "where store_id = 1" --preserve
```

Or truncate them

```sh
pgsync products "where store_id = 1" --truncate
```

### Exclude Tables

```sh
pgsync --exclude users
```

To always exclude, add to `.pgsync.yml`.

```yml
exclude:
  - table1
  - table2
```

For Rails, you probably want to exclude schema migrations.

```yml
exclude:
  - schema_migrations
```

### Groups

Define groups in `.pgsync.yml`:

```yml
groups:
  group1:
    - table1
    - table2
```

And run:

```sh
pgsync group1
```

You can also use groups to sync a specific record and associated records in other tables.

To get product `123` with its reviews, last 10 coupons, and store, use:

```yml
groups:
  product:
    products: "where id = {id}"
    reviews: "where product_id = {id}"
    coupons: "where product_id = {id} order by created_at desc limit 10"
    stores: "where id in (select store_id from products where id = {id})
```

And run:

```sh
pgsync product:123
```

### Schema

Sync schema

```sh
pgsync --schema-only
```

Specify tables

```sh
pgsync table1,table2 --schema-only
```

## Sensitive Information

Prevent sensitive information - like passwords and email addresses - from leaving the remote server.

Define rules in `.pgsync.yml`:

```yml
data_rules:
  email: unique_email
  last_name: random_letter
  birthday: random_date
  users.auth_token:
    value: secret
  visits_count:
    statement: "(RANDOM() * 10)::int"
  encrypted_*: null
```

`last_name` matches all columns named `last_name` and `users.last_name` matches only the users table. Wildcards are supported, and the first matching rule is applied.

Options for replacement are:

- null
- value
- statement
- unique_email
- unique_phone
- random_letter
- random_int
- random_date
- random_time
- random_ip
- untouched

## Multiple Databases

To use with multiple databases, run:

```sh
pgsync --setup db2
```

This creates `.pgsync-db2.yml` for you to edit. Specify a database in commands with:

```sh
pgsync --db db2
```

## Safety

To keep you from accidentally overwriting production, the destination is limited to `localhost` or `127.0.0.1` by default.

To use another host, add `to_safe: true` to your `.pgsync.yml`.

## Setup Scripts

Use groups when possible to take advantage of parallelism.

For Ruby scripts, you may need to do:

```rb
Bundler.with_clean_env do
  system "pgsync ..."
end
```

## Upgrading

Run:

```sh
gem install pgsync
```

To use master, run:

```sh
gem install specific_install
gem specific_install ankane/pgsync
```

## Thanks

Inspired by [heroku-pg-transfer](https://github.com/ddollar/heroku-pg-transfer).

## TODO

- Support for schemas other than `public`

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgsync/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgsync/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features
