# Docker image for airflow scheduling of extracting data and putting it into S3
FROM apache/airflow:latest-python3.9

WORKDIR /usr/local/airflow

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /usr/local/airflow/orchestration
COPY dags /opt/airflow/dags

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/orchestration"

COPY airflow_setup.sh /usr/local/bin/airflow_setup.sh


EXPOSE 8080