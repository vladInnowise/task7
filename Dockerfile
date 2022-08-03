FROM python:latest

WORKDIR /app

COPY ./functions.py .
COPY ./main.py .
COPY ./.env .
COPY ./config.py .

RUN pip install pandas requests

CMD ["python", "-u", "./main.py"]