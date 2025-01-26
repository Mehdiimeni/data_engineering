
FROM bitnami/spark:3.1.1


WORKDIR /app


COPY py_spark_app.py .


CMD ["/opt/bitnami/scripts/spark/entrypoint.sh", "spark-submit", "/app/py_spark_app.py"]


