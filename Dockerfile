FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

VOLUME [ "/app/data", "/app/logs" ]

CMD ["python", "main.py"]
