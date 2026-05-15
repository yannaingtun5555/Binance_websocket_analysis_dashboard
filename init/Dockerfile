FROM python:3.11-slim

WORKDIR /init

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "topic_creation.py"]