FROM python:3.11-slim

RUN apt update && \
    apt upgrade -y && \
    apt install -y git

WORKDIR /app
COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]