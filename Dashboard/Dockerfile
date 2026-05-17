FROM python:3.11-slim

WORKDIR /Dashboard

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8501

ENTRYPOINT ["streamlit", "run", "streamlit.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.enableCORS=false", "--server.enableXsrfProtection=false"]