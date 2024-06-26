FROM ruby:3.1.6-alpine3.20

MAINTAINER Andrew Kane <andrew@ankane.org>

RUN apk add --update build-base libpq-dev postgresql-client && \
    gem install pgsync && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgsync"]
