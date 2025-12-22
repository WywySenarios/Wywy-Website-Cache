#!/bin/bash
# Check if an argument is provided
if [ -z "$1" ]; then
  echo "Error: No argument provided."
  echo "Usage: $0 <sync|recv|pgres>"
  exit 1
fi

case "$1" in
  sync)
    sudo docker run -it --rm \
    -p 2523:2523 \
    --env-file .env \
    --mount type=bind,source="$(pwd)/../secrets/admin.txt",target=/run/secrets/admin,readonly \
    docker-sync /bin/sh
    ;;
  mod)
    sudo docker run -it --rm \
    -p 2325:2325 \
    --env-file .env \
    --mount type=bind,source="$(pwd)/../secrets/admin.txt",target=/run/secrets/admin,readonly \
    docker-mod /bin/sh
    ;;
  pgres)
    sudo docker run -it --rm -p 5432:5432 -v "postgres-db:/var/lib/postgresql" docker-postgres /bin/sh
    ;;
  *)
    echo "Error: Invalid argument '$1'. Expected 'astro', 'sqlr', or 'pgres'."
    exit 1
    ;;
esac
