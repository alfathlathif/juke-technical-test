FROM apache/airflow:2.10.1  

RUN pip install --upgrade pip

USER root

RUN apt-get update && apt-get install -y gosu \
    && pip install pendulum

USER airflow
