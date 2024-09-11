FROM apache/airflow:2.10.1
RUN pip install --upgrade pip
USER root
RUN apt-get update
RUN apt-get install wget