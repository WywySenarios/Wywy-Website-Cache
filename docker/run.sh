#!/bin/bash
# Check if an argument is provided
if [ -z "$1" ]; then
  echo "Error: No argument provided."
  echo "Usage: $0 <prod | dev>"
  exit 1
fi

case "$1" in
    prod)
        sudo docker compose -f docker-compose.prod.yml up
        ;;
    dev)
        sudo docker compose -f docker-compose.dev.yml up --watch
        ;;
    *)
        echo "Error: Invalid argument '$1'. Expected 'prod' or 'dev'"
        exit 1
        ;;
esac