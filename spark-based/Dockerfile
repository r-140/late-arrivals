FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip check
COPY . .
ENV SPARK_LOCAL_IP=127.0.0.1 PYTHONUNBUFFERED=1
# Resolve Delta JARs once at image build, not at every demo startup.
RUN python app/cache_jars.py
CMD ["python", "app/server.py"]
