FROM python:3.10-slim

WORKDIR /app

COPY spac.py .
COPY dmi.py .
COPY connection.py .

RUN pip install psycopg2-binary
RUN pip install requests
#RUN pip install pandas
#RUN pip install numpy

CMD ["python", "spac.py"]