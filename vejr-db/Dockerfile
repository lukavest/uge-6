FROM python:3.10-slim

WORKDIR /app

COPY spac-data.py .
COPY connection.py .

RUN pip install psycopg2-binary
RUN pip install requests

CMD ["python", "spac-data.py"]