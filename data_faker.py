from faker import Faker
import random
import datetime

faker = Faker()

def generate_orders(n):
    return [{
        'id': i,
        'title': faker.word(),
        'created_at': faker.date_time_this_year()
    }for i in range(1,n+1)]


def generate_order_products(order_ids, product_ids):
    data = []
    for order_id in order_ids:
        num_products = random.randint(1, 5)
        selected_products = random.sample(product_ids, num_products)
        for product_id in selected_products:
            data.append({
                "id": len(data) + 1,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": random.randint(1, 10),
                "created_at": faker.date_time_this_year()
            })
    return data