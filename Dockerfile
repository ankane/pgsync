FROM ruby:3.1.1-alpine3.15

MAINTAINER Andrew Kane <andrew@ankane.org>

RUN apk add --update libpq postgresql-client ruby-dev build-base libxml2-dev libxslt-dev pcre-dev libffi-dev postgresql-dev && \
    gem install pgsync && \
    apk del ruby-dev build-base libxml2-dev libxslt-dev pcre-dev libffi-dev postgresql-dev && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgsync"]
