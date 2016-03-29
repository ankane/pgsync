# pgsync

Quickly and securely sync data between environments

## Installation

```sh
gem install pgsync
```

And in your project directory, run:

```sh
pgsync setup
```

This creates `.pgsync.yml` for you to customize. We recommend checking this into your version control (assuming it doesn’t contain sensitive information).

## How to Use

Fetch all tables

```sh
pgsync
```

Fetch specific tables

```sh
pgsync table1,table2
```

Fetch specific rows (truncates destination table first)

```sh
pgsync products --where "id < 100"
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

### Schema

Fetch schema

```sh
pgsync schema
```

Specify tables

```sh
pgsync schema table1,table2
```

### Groups

Define groups in `.pgsync.yml`:

```yml
groups:
  group1:
    - table1
    - table2
  group2:
    - table3
    - table4
```

And run:

```sh
pgsync groups group1,group2
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

To use with multiple databases, edit your config file and add another entry:

```sh
another_db:
  from: $(heroku config:get ANOTHER_DATABASE_URL)
  to: postgres://localhost:5432/myanotherapp_development
```

## Safety

To keep you from accidentally overwriting production, the destination is limited to `localhost` or `127.0.0.1` by default.

To use another host, add `to_safe: true` to your `.pgsync.yml`.

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
