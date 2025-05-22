FROM apache/airflow:2.11.0
COPY requirements.txt /
COPY src /opt/airflow/src

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/src"
RUN pip install --no-cache-dir "apache-airflow==2.11.0" -r /requirements.txt