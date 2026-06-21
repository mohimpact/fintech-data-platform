FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        default-jre-headless \
        curl \
        procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONPATH=/app/src
ENV PYSPARK_PYTHON=python3
ENV SPARK_MASTER_URL=local[*]

WORKDIR /app

COPY requirements/streaming.txt /tmp/streaming-requirements.txt
COPY requirements/lakehouse.txt /tmp/lakehouse-requirements.txt
RUN pip install --no-cache-dir \
        -r /tmp/streaming-requirements.txt \
        -r /tmp/lakehouse-requirements.txt

COPY src/fintech_platform /app/src/fintech_platform
RUN mkdir -p /app/checkpoints

CMD ["python", "-m", "fintech_platform.streaming"]
