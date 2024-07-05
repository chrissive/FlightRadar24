#!/bin/bash

# Initialize the database
superset db upgrade

# Create an admin user if it doesn't exist
if ! superset fab list-users | grep -q admin; then
  export FLASK_APP=superset
  superset fab create-admin \
      --username admin \
      --firstname Admin \
      --lastname User \
      --email admin@example.com \
      --password admin
fi

# Setup roles
superset init

# Start the Superset server
superset run -p 8088 --with-threads --reload --debugger
