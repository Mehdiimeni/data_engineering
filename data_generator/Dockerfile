FROM python:3.9-slim

WORKDIR /app

COPY data_faker.py .

RUN pip install mysql-connector-python faker

CMD ["python", "data_faker.py"]
