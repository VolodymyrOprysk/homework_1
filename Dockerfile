FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY data/ ./data/
COPY scripts/ ./scripts/
COPY img/ ./img/

RUN chmod +x src/*.py

ENV DB_HOST=mysql-etl
ENV DB_PORT=3306
ENV DB_NAME=etl_db
ENV DB_USER=etl_user
ENV DB_PASSWORD=etlpass

ENV PYTHONPATH=/app/src:/app

CMD ["tail", "-f", "/dev/null"]