FROM apache/airflow:2.9.1

# Update container
USER root
RUN apt-get update && apt-get install -y git

# Install airflow dependencies
USER airflow