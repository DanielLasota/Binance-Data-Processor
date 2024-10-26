FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/logs

VOLUME [ "/app/data", "/app/logs" ]

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "binance_archiver/main_data_sink.py"]
