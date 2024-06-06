#!/bin/bash

# Install dependencies
pip install -r airflow_dags/requirements.txt

# Run the Airflow scheduler and webserver in parallel
airflow scheduler & airflow webserver &

# Wait for background processes to finish
wait
