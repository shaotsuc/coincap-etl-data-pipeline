# Base image
FROM apache/airflow:2.7.1

ENV AIRFLOW_HOME=/opt/airflow

# Switch to the airflow user
USER airflow

WORKDIR $AIRFLOW_HOME

# Copy the files into the container and package dependencies
COPY . .
RUN pip install -r requirements.txt