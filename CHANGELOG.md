## 0.6.0 (unreleased)

- Added message for different column types

Breaking

- Dropped support for Postgres < 9.5
- Removed deprecated options
- Raise an error when `--config` or `--db` option provided and config not found

## 0.5.5 (2020-05-13)

- Added `--jobs` option
- Added `--defer-constraints` option
- Added `--disable-user-triggers` option
- Added `--disable-integrity` option
- Improved error message for older libpq

## 0.5.4 (2020-05-09)

- Fixed output for `--in-batches`

## 0.5.3 (2020-04-03)

- Improved Postgres error messages
- Fixed behavior of wildcard without schema

## 0.5.2 (2020-03-27)

- Added `--fail-fast` option
- Automatically exclude tables when `--init` run inside Rails app
- Improved error message
- Fixed typo in error message

## 0.5.1 (2020-03-26)

- Fixed Slop warning with Ruby 2.7

## 0.5.0 (2020-03-26)

- Improved output when syncing
- Improved output on interrupt
- Added `--no-sequences` option

## 0.4.3 (2019-10-27)

- Added `sslmode` to template

## 0.4.2 (2019-10-27)

- Improved flexibility of commands
- Sync all objects when no tables specified

## 0.4.1 (2018-12-15)

- Made `psql` version check more robust
- Fixed issue with non-lowercase primary key
- Prefer `--init` over `--setup`
- Improved data rules

## 0.4.0 (2018-02-28)

- Sync all schemas in search path by default
- Added support for socket connections
- Added support for environment variables

## 0.3.9 (2018-02-27)

- Better support for schemas
- Added `--schemas` option
- Added `--all-schemas` option
- Added `--schema-first` option
- Fixed issue with non-lowercase tables and partial syncs

## 0.3.8 (2017-10-01)

- Added Windows support
- Added `random_string` and `random_number` replacement options
- Improved performance of `--in-batches` for large tables

## 0.3.7 (2017-08-30)

- Fixed non-lowercase tables and columns
- Fixed `--truncate` option with `--in-batches`

## 0.3.6 (2016-10-02)

- Fixed `Table does not exist in source` error

## 0.3.5 (2016-07-23)

- Support schemas other than public

## 0.3.4 (2016-04-29)

- Added `--in-batches` mode for production transfers with `--batch-size` and `--sleep`

## 0.3.3 (2016-04-25)

- Added `-d` option as an alias for `--db`
- Added support for wildcard tables
- Fixed `--schema-only` errors

## 0.3.2 (2016-04-19)

- Prefer `{1}` for interpolation
- Added `--overwrite` option
- Deprecated `--where` and `--limit`

## 0.3.1 (2016-04-06)

- Added `-t` or `--tables`, `-g` or `--groups` options
- Deprecated `tables`, `groups`, and `setup` commands

## 0.3.0 (2016-04-06)

- More powerful groups
- Overwrite rows by default when `WHERE` clause (previously truncated)
- Added `pgsync users "WHERE id = 1`
- Added `pgsync group1`, shorthand for `pgsync groups group1`
- Added `--schema-only` option
- Added `--no-rules` option
- Added `--setup` option
- Added `--truncate` option

## 0.2.4 (2016-04-04)

- Added `--preserve` option
- Added `--list` option for groups and tables
- Added `--limit` option

## 0.2.3 (2016-03-30)

- Fixed `no PostgreSQL user name specified in startup packet`

## 0.2.2 (2016-03-28)

- Added `--debug` option

## 0.2.1 (2016-03-27)

- Do not require config file

## 0.2.0 (2016-03-26)

- Fixed idle transaction timeout errors - respects `statement_timeout` as a result
- Raise error when command exits with non-zero status

## 0.1.1 (2016-03-23)

- Better support for multiple databases
- Search parent directories for config file

## 0.1.0 (2015-12-07)

- First release
