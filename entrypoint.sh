#!/usr/bin/env bash
airflow db upgrade

airflow users create -r Admin -u admin -p admin -e admin@example.com -f admin -l airflow
# "$_AIRFLOW_WWW_USER_USERNAME" -p "$_AIRFLOW_WWW_USER_PASSWORD"

airflow webserver


# export env VAR
export POSTGRES_USER=${POSTGRES_USER}
export POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
export DB_NAME=${DB_NAME}