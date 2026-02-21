FROM postgres:18 AS db
WORKDIR apps/postgres

USER root

RUN apt-get update && \
    apt-get install -y postgis && \
    rm -rf /var/lib/apt/lists/*

USER postgres