FROM python:3.14.0-slim-bookworm
WORKDIR /apps/create_tables

COPY ./apps/create_tables .
COPY ./config.yml .

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential \
#     libpq-dev \
#     gcc \
#     && rm -rf /var/lib/apt/lists/*
RUN rm -rf /var/lib/apt/lists/*

RUN pip install psycopg2-binary
RUN pip install --no-cache-dir PyYAML

CMD ["python3", "create_tables.py"]