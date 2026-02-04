FROM python:3.14.0-slim-bookworm
WORKDIR /apps/sync

# create non-root user
RUN useradd -m app

# populate files
COPY --chown=app:app ./apps/sync .

# install python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

# enter as non-root user
USER app

EXPOSE 8000

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]