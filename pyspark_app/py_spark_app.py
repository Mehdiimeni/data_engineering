from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, collect_list, current_timestamp
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("BatchProcessingWithNestedArray") \
    .config("spark.jars", "C:\\tools\\mysql-connector-j-9.1.0.jar") \
    .getOrCreate()

batch_interval = 60 * 60  
start_time = "2025-01-01 00:00:00"
end_time = "2025-01-01 23:59:59"

current_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

while current_time < datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S"):
    next_time = current_time + timedelta(seconds=batch_interval)

    orders_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/data_engineering",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=f"(SELECT * FROM Orders WHERE created_at >= '{current_time}' AND created_at < '{next_time}') AS batch_orders",
        user="root",
        password="",
    ).load()

    order_products_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/data_engineering",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable=f"(SELECT * FROM OrderProducts WHERE created_at >= '{current_time}' AND created_at < '{next_time}') AS batch_order_products",
        user="root",
        password="",
    ).load()

    products_df = spark.read.format("jdbc").options(
        url="jdbc:mysql://localhost:3306/data_engineering",
        driver="com.mysql.cj.jdbc.Driver",
        dbtable="Products",
        user="root",
        password="",
    ).load()

    # Join tables
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

    # Add a new column 'products' with a nested array structure
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
