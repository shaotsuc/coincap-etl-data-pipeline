# Base image
FROM apache/airflow:2.7.1

FROM python:3.10-slim-bullseye

ENV AIRFLOW_HOME=/opt/airflow

# Switch to the airflow user
USER airflow

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID


# Copy the files into the container and package dependencies
COPY . .
RUN pip install -r requirements.txt
RUN chmod +x ./entrypoint.sh


# Add dbt to PATH
# ENV PATH="/root/.local/bin:${PATH}"
