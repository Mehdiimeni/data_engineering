from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct


spark = SparkSession.builder \
    .appName("DataEngineeringPipeline") \
    .config("spark.jars", "C:\\tools\\mysql-connector-j-9.1.0.jar") \
    .getOrCreate()


orders_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_engineering",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="Orders",
    user="root",
    password=""
).load()

order_products_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_engineering",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="OrderProducts",
    user="root",
    password=""
).load()

products_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_engineering",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="Products",
    user="root",
    password=""
).load()


df = orders_df.join(order_products_df, orders_df.id == order_products_df.order_id) \
    .join(products_df, order_products_df.product_id == products_df.id) \
    .select(
        orders_df.id,
        orders_df.total_price,
        orders_df.created_at,
        struct(
            products_df.title,
            order_products_df.quantity,
            products_df.id.alias("product_id")
        ).alias("products")
    )


df.show(truncate=False)
