#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run Airflow scheduler and webserver in parallel
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Start the scheduler and webserver in the background
airflow scheduler & 
airflow webserver -p 8080 &
