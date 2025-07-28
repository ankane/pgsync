FROM ruby:3-alpine

LABEL org.opencontainers.image.authors="Andrew Kane <andrew@ankane.org>"

RUN apk add --update build-base libpq-dev postgresql-client && \
    gem install pg --platform ruby && \
    gem install pgsync && \
    apk del build-base && \
    rm -rf /var/cache/apk/*

ENTRYPOINT ["pgsync"]
