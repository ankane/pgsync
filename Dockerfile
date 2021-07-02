FROM ruby:3.0.1-alpine3.13

MAINTAINER Andrew Kane <andrew@ankane.org>

RUN apk add --update ruby-dev build-base \
  libxml2-dev libxslt-dev pcre-dev libffi-dev \
  postgresql-dev \
  # May need pg_dump and pg_restore commands to sync schema
  postgresql-client \
  # May need git to detect `heroku` usage
  git

RUN gem install pgsync

ENTRYPOINT ["pgsync"]
