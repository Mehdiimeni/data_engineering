from faker import Faker
import random
import mysql.connector
from datetime import datetime

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',
    database='data_engineering'
)
cursor = conn.cursor()

fake = Faker()

products = []
for i in range(1, 100):
    product = {
        'id': i,
        'title': fake.word(),
        'created_at': fake.date_this_year()
    }
    products.append(product)
    cursor.execute("INSERT INTO Products (id, title, created_at) VALUES (%s, %s, %s)",
                   (product['id'], product['title'],  product['created_at']))
conn.commit()

orders = []
for i in range(1, 60):
    order = {
        'id': i,
        'total_price': random.uniform(50, 500),
        'created_at': fake.date_this_year()
    }
    orders.append(order)
    cursor.execute("INSERT INTO Orders (id, total_price, created_at) VALUES (%s, %s, %s)",
                   (order['id'], order['total_price'], order['created_at']))
conn.commit()

for order in orders:
    order_id = order['id']
    for _ in range(random.randint(1, 6)):
        product = random.choice(products)
        quantity = random.randint(1, 5)
        cursor.execute("INSERT INTO OrderProducts (order_id, product_id, quantity, created_at) VALUES (%s, %s, %s, %s)",
                       (order_id, product['id'], quantity, order['created_at']))
conn.commit()

cursor.close()
conn.close()
