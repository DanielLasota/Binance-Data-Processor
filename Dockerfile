FROM python:3.11-slim

RUN apt-get update && apt-get install -y curl

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

VOLUME [ "/app/data", "/app/logs" ]

# EXPOSE 80

ENV PYTHONPATH=/app
ENV FLASK_APP=main.py
# ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_HOST=127.0.0.1

CMD ["python", "binance_archiver/main.py"]
