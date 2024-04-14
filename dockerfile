FROM python:3.8-slim

RUN apt update && \
    apt upgrade -y && \
    apt install -y git

WORKDIR /app
COPY . /app
RUN pip install -r requirements.txt

CMD ["python", "./my_app.py"]