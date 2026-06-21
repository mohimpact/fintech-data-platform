FROM python:3.11-slim

ENV PYTHONPATH=/app/src
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none

WORKDIR /app

COPY requirements/analytics.txt /tmp/analytics-requirements.txt
RUN pip install --no-cache-dir -r /tmp/analytics-requirements.txt

COPY src/fintech_platform /app/src/fintech_platform

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "src/fintech_platform/analytics/app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
