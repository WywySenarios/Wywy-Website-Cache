FROM python:3.14.0-slim-bookworm
WORKDIR /apps/create_tables

RUN rm -rf /var/lib/apt/lists/*

RUN pip install psycopg2-binary
RUN pip install --no-cache-dir PyYAML

COPY ./apps/create_tables .
COPY ./config.yml .

CMD ["python3", "create_tables.py"]