FROM python:3.14.0-slim-bookworm
WORKDIR /apps/sync

# create non-root user
RUN groupadd -g 2523 Wywy-Website
RUN useradd -m app -g 2523 -u 25232

# populate files
COPY --chown=sappync:app ./apps/sync .

# prepare logging directory
RUN mkdir -p /var/lib/Wywy-Website/cache
RUN chown app /var/lib/Wywy-Website/cache
RUN chmod 700 /var/lib/Wywy-Website/cache

# install python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

# enter as non-root user
USER app

EXPOSE 8000

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]