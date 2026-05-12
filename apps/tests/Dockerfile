FROM python:3.12-slim

ARG USER_ID
WORKDIR /home/wywy

# create non-root user
RUN groupadd -g 2523 Wywy-Website
RUN useradd -m -d /home/wywy -g Wywy-Website -u ${USER_ID} wywy

# populate files via volume
COPY --chown=tests:Wywy-Website /apps/sync/requirements.txt /tmp/requirements.txt

# prepare cache directory
RUN mkdir -p /var/lib/Wywy-Website/cache
RUN chown wywy /var/lib/Wywy-Website/cache
RUN chmod 700 /var/lib/Wywy-Website/cache
RUN echo "{}" > /var/lib/Wywy-Website/cache/cache.json
RUN chown wywy /var/lib/Wywy-Website/cache/cache.json
RUN chmod 700 /var/lib/Wywy-Website/cache/cache.json

# prepare logging directory
RUN mkdir -p /var/log/Wywy-Website/cache
RUN chown wywy /var/log/Wywy-Website/cache
RUN chmod 700 /var/log/Wywy-Website/cache

# install required python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*
RUN pip install -r /tmp/requirements.txt

# enter as non-root user
USER wywy

CMD ["python", "manage.py", "test"]