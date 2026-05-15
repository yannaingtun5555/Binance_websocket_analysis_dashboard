FROM python:3.11-slim

WORKDIR /consumer

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

#CMD ["python", "consumer.py"]