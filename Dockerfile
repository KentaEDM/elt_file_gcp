FROM apache/airflow:2.6.3-python3.10

USER root

RUN apt-get update && \
    apt-get -y install git &&\
    apt-get clean

COPY requirements.txt /requirements.txt
USER airflow
RUN pip install -r /requirements.txt
USER airflow