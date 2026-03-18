FROM python:3.10-slim

WORKDIR /app

COPY etl ./etl
COPY main.py .
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

#CMD ["python", "main.py", "all"]

CMD ["python", "-m", "etl.server.server"]