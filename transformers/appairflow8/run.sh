#!/bin/bash

# Step a: Install dependencies
pip install -r requirements.txt

# Step b: Run all necessary parts of the codebase
# Initialize the Airflow database
airflow db init

# Start the Airflow web server in the background
airflow webserver &

# Start the Airflow scheduler in the background
airflow scheduler &

# Wait for background processes to start
sleep 10

# Trigger the DAG
airflow dags trigger azure_file_share_to_blob

# Wait for all background processes to complete
wait
