from faker import Faker
import random
import mysql.connector
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DataGenerator:
    def __init__(self, db_config):
        self.fake = Faker()
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect_db(self):
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            logging.info("Connected to the database.")
        except mysql.connector.Error as err:
            logging.error(f"Database connection failed: {err}")
            raise

    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def generate_products(self, count=100):
        return [
            {'id': i, 'title': self.fake.word(), 'created_at': self.fake.date_this_year()}
            for i in range(1, count + 1)
        ]

    def insert_products(self, products):
        try:
            self.cursor.executemany(
                "INSERT INTO Products (id, title, created_at) VALUES (%s, %s, %s)",
                [(p['id'], p['title'], p['created_at']) for p in products]
            )
            self.conn.commit()
            logging.info(f"{len(products)} products inserted into the database.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to insert products: {err}")
            raise

    def generate_orders(self, count=60):
        return [
            {'id': i, 'total_price': round(random.uniform(50, 500), 2), 'created_at': self.fake.date_this_year()}
            for i in range(1, count + 1)
        ]

    def insert_orders(self, orders):
        try:
            self.cursor.executemany(
                "INSERT INTO Orders (id, total_price, created_at) VALUES (%s, %s, %s)",
                [(o['id'], o['total_price'], o['created_at']) for o in orders]
            )
            self.conn.commit()
            logging.info(f"{len(orders)} orders inserted into the database.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to insert orders: {err}")
            raise

    def generate_order_products(self, orders, products):
        try:
            data = []
            for order in orders:
                for _ in range(random.randint(1, 6)):
                    product = random.choice(products)
                    quantity = random.randint(1, 5)
                    data.append((order['id'], product['id'], quantity, order['created_at']))
            self.cursor.executemany(
                "INSERT INTO OrderProducts (order_id, product_id, quantity, created_at) VALUES (%s, %s, %s, %s)", data
            )
            self.conn.commit()
            logging.info(f"{len(data)} order-product entries inserted into the database.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to insert order products: {err}")
            raise

    def run(self):
        try:
            self.connect_db()
            products = self.generate_products()
            self.insert_products(products)
            orders = self.generate_orders()
            self.insert_orders(orders)
            self.generate_order_products(orders, products)
        finally:
            self.close_db()


if __name__ == "__main__":
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': '',
        'database': 'data_engineering'
    }

    generator = DataGenerator(db_config)
    generator.run()
