FROM apache/airflow
RUN pip install --upgrade pip
USER root
RUN apt-get update && apt-get install -y gosu
