#!/bin/sh
set -e
exec airflow webserver -D --port 8080 &
exec airflow scheduler 
