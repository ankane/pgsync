# pgsync

Sync data from one Postgres database to another (like `pg_dump`/`pg_restore`). Designed for:

- **speed** - tables are transferred in parallel
- **security** - built-in methods to prevent sensitive data from ever leaving the server
- **flexibility** - gracefully handles schema differences, like missing columns and extra columns
- **convenience** - sync partial tables, groups of tables, and related records

:tangerine: Battle-tested at [Instacart](https://www.instacart.com/opensource)

[![Build Status](https://travis-ci.org/ankane/pgsync.svg?branch=master)](https://travis-ci.org/ankane/pgsync)

## Installation

pgsync is a command line tool. To install, run:

```sh
gem install pgsync
```

This will give you the `pgsync` command. If installation fails, you may need to install [dependencies](#dependencies).

## Setup

In your project directory, run:

```sh
pgsync --init
```

This creates `.pgsync.yml` for you to customize. We recommend checking this into your version control (assuming it doesn’t contain sensitive information). `pgsync` commands can be run from this directory or any subdirectory.

## How to Use

Sync all tables

```sh
pgsync
```

**Note:** pgsync assumes your schema is setup in your `to` database. See the [schema section](#schema) if that’s not the case.

Sync specific tables

```sh
pgsync table1,table2
```

Works with wildcards as well

```sh
pgsync "public.*"
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

## Exclude Tables

```sh
pgsync --exclude users
```

To always exclude, add to `.pgsync.yml`.

```yml
exclude:
  - table1
  - table2
```

For Rails, you probably want to exclude schema migrations and Active Record metadata.

```yml
exclude:
  - schema_migrations
  - ar_internal_metadata
```

## Groups

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
    products: "where id = {1}"
    reviews: "where product_id = {1}"
    coupons: "where product_id = {1} order by created_at desc limit 10"
    stores: "where id in (select store_id from products where id = {1})"
```

And run:

```sh
pgsync product:123
```

## Schema

**Note:** pgsync is designed to sync data. You should use a schema migration tool to manage schema changes. The methods in this section are provided for convenience but not recommended.

Sync schema before the data

```sh
pgsync --schema-first
```

**Note:** This wipes out existing data

Specify tables

```sh
pgsync table1,table2 --schema-first
```

Or just the schema

```sh
pgsync --schema-only
```

pgsync does not try to sync Postgres extensions.

## Sensitive Data

Prevent sensitive data like email addresses from leaving the remote server.

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

- `unique_email`
- `unique_phone`
- `unique_secret`
- `random_letter`
- `random_int`
- `random_date`
- `random_time`
- `random_ip`
- `value`
- `statement`
- `null`
- `untouched`

Rules starting with `unique_` require the table to have a primary key. `unique_phone` requires a numeric primary key.

## Foreign Keys

Foreign keys can make it difficult to sync data. Three options are:

1. Manually specify the order of tables
2. Use deferrable constraints
3. Disable foreign key triggers, which can silently break referential integrity

When manually specifying the order, use `--jobs 1` so tables are synced one-at-a-time.

```sh
pgsync table1,table2,table3 --jobs 1
```

If your tables have [deferrable constraints](https://begriffs.com/posts/2017-08-27-deferrable-sql-constraints.html), use:

```sh
pgsync --defer-constraints
```

To disable foreign key triggers and potentially break referential integrity, use:

```sh
pgsync --disable-integrity
```

## Triggers

Disable user triggers with:

```sh
pgsync --disable-user-triggers
```

## Append-Only Tables

For extremely large, append-only tables, sync in batches.

```sh
pgsync large_table --in-batches
```

The script will resume where it left off when run again, making it great for backfills.

## Connection Security

Always make sure your [connection is secure](https://ankane.org/postgres-sslmode-explained) when connecting to a database over a network you don’t fully trust. Your best option is to connect over SSH or a VPN. Another option is to use `sslmode=verify-full`. If you don’t do this, your database credentials can be compromised.

## Safety

To keep you from accidentally overwriting production, the destination is limited to `localhost` or `127.0.0.1` by default.

To use another host, add `to_safe: true` to your `.pgsync.yml`.

## Multiple Databases

To use with multiple databases, run:

```sh
pgsync --init db2
```

This creates `.pgsync-db2.yml` for you to edit. Specify a database in commands with:

```sh
pgsync --db db2
```

## Other Commands

Help

```sh
pgsync --help
```

Version

```sh
pgsync --version
```

List tables

```sh
pgsync --list
```

## Scripts

Use groups when possible to take advantage of parallelism.

For Ruby scripts, you may need to do:

```rb
Bundler.with_unbundled_env do
  system "pgsync ..."
end
```

## Dependencies

If installation fails, your system may be missing Ruby or libpq.

On Mac, run:

```sh
brew install postgresql
```

On Ubuntu, run:

```sh
sudo apt-get install ruby-dev libpq-dev build-essential
```

## Upgrading

Run:

```sh
gem install pgsync
```

To use master, run:

```sh
gem install specific_install
gem specific_install https://github.com/ankane/pgsync.git
```

## Related Projects

Also check out:

- [Dexter](https://github.com/ankane/dexter) - The automatic indexer for Postgres
- [PgHero](https://github.com/ankane/pghero) - A performance dashboard for Postgres
- [pgslice](https://github.com/ankane/pgslice) - Postgres partitioning as easy as pie

## Thanks

Inspired by [heroku-pg-transfer](https://github.com/ddollar/heroku-pg-transfer).

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/pgsync/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/pgsync/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/pgsync.git
cd pgsync
bundle install

createdb pgsync_test1
createdb pgsync_test2
createdb pgsync_test3

bundle exec rake test
```
