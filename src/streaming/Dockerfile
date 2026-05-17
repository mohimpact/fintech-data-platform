# ─────────────────────────────────────────────────────────────────────────────
# Self-contained PySpark Streaming Job Image
# Base: python:3.11-slim + OpenJDK 11 (temurin)
# No dependency on bitnami or apache/spark Docker Hub images.
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim

# ── System deps: Java 11 (required by PySpark / Spark JVM) ────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-21-jre-headless \
        curl \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

# ── Python deps ────────────────────────────────────────────────────────────
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ── App ───────────────────────────────────────────────────────────────────
WORKDIR /app
COPY streaming_job.py .

# Checkpoints stored here (bind-mounted via docker-compose)
RUN mkdir -p /app/checkpoints

# ── Spark config: run in local mode (all cores on this container) ─────────
ENV SPARK_MASTER_URL=local[*]
ENV PYSPARK_PYTHON=python3

# Run via spark-submit bundled with pyspark pip package
CMD ["python", "/app/streaming_job.py"]
