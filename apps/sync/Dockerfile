FROM python:3.14.0-slim-bookworm
WORKDIR /home/wywy

ARG USER_ID

# create non-root user
RUN groupadd -g 2523 Wywy-Website
RUN useradd -m -d /home/wywy -g Wywy-Website -u ${USER_ID} wywy

# populate files
COPY --chown=wywy:Wywy-Website ./apps/sync /home/wywy

# prepare logging directory
RUN mkdir -p /var/lib/Wywy-Website/cache
RUN chown wywy /var/lib/Wywy-Website/cache
RUN chmod 700 /var/lib/Wywy-Website/cache

# install python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

# enter as non-root user
USER wywy

EXPOSE 8000

CMD ["python3", "manage.py", "runserver", "0.0.0.0:8000"]