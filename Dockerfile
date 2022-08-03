FROM python:latest

WORKDIR /app

COPY ./functions.py .
COPY ./main.py .

RUN pip install pandas requests

CMD ["python", "-u", "./main.py"]