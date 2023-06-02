# Docker image for airflow scheduling of extracting data and putting it into S3
FROM apache/airflow:latest-python3.9

ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV S3_BUCKET=${S3_BUCKET}

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY dags /opt/airflow/dags

RUN airflow db init

EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]