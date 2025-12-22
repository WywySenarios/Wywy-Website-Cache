FROM python:3.14.0-slim-bookworm
WORKDIR /apps/sync

COPY ./apps/sync/requirements.txt .
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

RUN useradd -m app
USER app

COPY --chown=app:app ./apps/sync .
COPY --chown=app:app ./config.yml .

EXPOSE 8000

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]