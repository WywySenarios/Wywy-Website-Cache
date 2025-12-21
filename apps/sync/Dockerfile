FROM python:3.14.0-slim-bookworm
WORKDIR /apps/sync

COPY ./apps/sync .
COPY ./config.yml .

RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

CMD ["python3", "create_tables.py"]