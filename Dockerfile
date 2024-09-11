FROM apache/airflow:2.7.3
RUN pip install --upgrade pip
USER root
RUN apt-get update
RUN apt-get install wget