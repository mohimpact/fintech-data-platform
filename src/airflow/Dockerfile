FROM apache/airflow:2.8.1-python3.10

USER root

# Install OpenJDK for PySpark
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Install PySpark matching our Lakehouse requirements
RUN pip install --no-cache-dir pyspark==3.5.1
