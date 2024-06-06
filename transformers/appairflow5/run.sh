#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run the Airflow scheduler and webserver in parallel
airflow scheduler & airflow webserver -p 8080
