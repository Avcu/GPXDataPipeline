FROM apache/airflow:3.3.0-python3.11

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow
RUN pip install --no-cache-dir gpxpy
RUN pip install --no-cache-dir fit2gpx
