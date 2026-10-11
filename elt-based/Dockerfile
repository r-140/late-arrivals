FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY --chown=50000:0 . .
ENV DBT_PROFILES_DIR=/app/dbt DBT_SEND_ANONYMOUS_USAGE_STATS=false
USER 50000
CMD ["python", "app/consumer.py"]
