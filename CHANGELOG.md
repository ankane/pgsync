# 0.2.5 [unreleased]

- Added `pgsync users "WHERE id = 1`
- Added `pgsync group1`, shorthand for `pgsync groups group1`
- Added `--schema-only` option
- Added `--no-rules` option

# 0.2.4

- Added `--preserve` option
- Added `--list` option for groups and tables
- Added `--limit` option

# 0.2.3

- Fixed `no PostgreSQL user name specified in startup packet`

# 0.2.2

- Added `--debug` option

# 0.2.1

- Do not require config file

# 0.2.0

- Fixed idle transaction timeout errors - respects `statement_timeout` as a result
- Raise error when command exits with non-zero status

# 0.1.1

- Better support for multiple databases
- Search parent directories for config file

# 0.1.0

- First release
