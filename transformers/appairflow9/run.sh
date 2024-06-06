#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run Airflow scheduler and webserver in parallel
airflow scheduler & airflow webserver &

# Wait for background processes to finish
wait
