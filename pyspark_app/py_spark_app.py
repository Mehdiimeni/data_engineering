import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, collect_list
from datetime import datetime, timedelta

class BatchProcessor:
    def __init__(self, spark_session, db_config, batch_interval, time_ranges):
        self.spark = spark_session
        self.db_config = db_config
        self.batch_interval = batch_interval
        self.time_ranges = time_ranges

    def load_data(self, table_name, time_column, start_time, end_time):
        query = f"(SELECT * FROM {table_name} WHERE {time_column} >= '{start_time}' AND {time_column} < '{end_time}') AS batch_data"
        return self.spark.read.format("jdbc").options(
            url=self.db_config["url"],
            driver=self.db_config["driver"],
            dbtable=query,
            user=self.db_config["user"],
            password=self.db_config["password"],
        ).load()

    def process_batch(self):
        for time_range in self.time_ranges:
            start_time = datetime.strptime(time_range["start"], "%Y-%m-%dT%H:%M:%S")
            end_time = datetime.strptime(time_range["end"], "%Y-%m-%dT%H:%M:%S")

            current_time = start_time
            while current_time < end_time:
                next_time = current_time + timedelta(seconds=self.batch_interval)

                orders_df = self.load_data("Orders", "created_at", current_time, next_time)
                order_products_df = self.load_data("OrderProducts", "created_at", current_time, next_time)
                products_df = self.spark.read.format("jdbc").options(
                    url=self.db_config["url"],
                    driver=self.db_config["driver"],
                    dbtable="Products",
                    user=self.db_config["user"],
                    password=self.db_config["password"],
                ).load()

                combined_df = orders_df.join(order_products_df, orders_df.id == order_products_df.order_id) \
                    .join(products_df, order_products_df.product_id == products_df.id) \
                    .select(
                        orders_df.id,
                        orders_df.total_price,
                        orders_df.created_at,
                        order_products_df.quantity,
                        products_df.title,
                        order_products_df.product_id
                    )

                final_df = combined_df.groupBy(
                    orders_df.id,
                    orders_df.total_price,
                    orders_df.created_at
                ).agg(
                    collect_list(
                        struct(
                            products_df.title.alias("title"),
                            order_products_df.quantity.alias("quantity"),
                            products_df.id.alias("product_id")
                        )
                    ).alias("products")
                )

                final_df.show(truncate=False)
                current_time = next_time

def load_config(config_path="config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config["time_ranges"]

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("BatchProcessingWithNestedArray") \
        .config("spark.jars", "./libs/mysql-connector-j-9.1.0.jar") \
        .getOrCreate()

    db_config = {
        "url": "jdbc:mysql://localhost:3306/data_engineering",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "root",
        "password": ""
    }

    time_ranges = load_config()

    batch_processor = BatchProcessor(
        spark_session=spark,
        db_config=db_config,
        batch_interval=60 * 60,  # Interval in seconds
        time_ranges=time_ranges
    )

    batch_processor.process_batch()
