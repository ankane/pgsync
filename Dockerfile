FROM ruby:3.0.1-alpine3.13

MAINTAINER Andrew Kane <andrew@ankane.org>

RUN apk add --update ruby-dev build-base \
  libxml2-dev libxslt-dev pcre-dev libffi-dev \
  postgresql-dev

RUN gem install pgsync

ENTRYPOINT ["pgsync"]
