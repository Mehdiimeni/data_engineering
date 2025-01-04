from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct


spark = SparkSession.builder \
    .appName("Data Engineering Pipeline") \
    .config("spark.jars", "C:\\tools\\mysql-connector-java-8.0.33.jar") \
    .getOrCreate()


orders_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_engineering",  
    driver="com.mysql.cj.jdbc.Driver",         
    dbtable="Orders",
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


order_products_df = spark.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/data_engineering",
    driver="com.mysql.cj.jdbc.Driver",
    dbtable="OrderProducts",
    user="root",
    password=""
).load()


final_df = orders_df.join(order_products_df, orders_df.id == order_products_df.order_id) \
    .join(products_df, products_df.id == order_products_df.product_id) \
    .groupBy("orders.id", "orders.total_price", "orders.created_at") \
    .agg(collect_list(struct("products.title", "order_products.quantity", "order_products.product_id")).alias("products"))


final_df.show(truncate=False)
