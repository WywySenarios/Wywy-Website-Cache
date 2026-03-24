FROM python:3.12-slim
WORKDIR /apps/sync

# populate files via volume
COPY ./apps/sync/requirements.txt /requirements.txt

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r /requirements.txt

CMD ["python3", "-m", "unittest", "discover", "-s", "/apps/sync"]