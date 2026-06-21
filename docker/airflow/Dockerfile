FROM apache/airflow:2.8.1-python3.10

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless \
  && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java
ENV PYTHONPATH=/opt/airflow/src

USER airflow

COPY requirements/lakehouse.txt /tmp/lakehouse-requirements.txt
RUN pip install --no-cache-dir -r /tmp/lakehouse-requirements.txt
