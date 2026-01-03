import random
from datetime import datetime, timedelta
import logging

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine

# 🚨 Import your custom MultiDBConnector class
from .db_connections import MultiDBConnector

# Initialize Faker and logging
fake = Faker('en_US')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ===============================================
# DATA VOLUME CONFIGURATION
# ===============================================
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 30
NUM_ORDERS = 5000
ORDERS_START_DATE = datetime(2025, 1, 1)
ORDERS_END_DATE = datetime(2025, 12, 31)


# ===============================================
# GENERATION FUNCTIONS
# ===============================================

def get_random_date(start_date, end_date):
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start_date + timedelta(seconds=random_second)


def generate_customers(n):
    logging.info(f"-> Generating {n} customers...")
    data = []
    fake.unique.clear()

    for i in range(1, n + 1):
        data.append({
            'customer_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'phone_number': fake.phone_number(),
            'registration_date': fake.date_between(start_date=datetime(2024, 1, 1),
                                                   end_date=ORDERS_START_DATE).strftime('%Y-%m-%d')
        })

    fake.unique.clear()
    return pd.DataFrame(data)


def generate_products(n):
    logging.info(f"-> Generating {n} products...")
    categories = ['Electronics', 'Apparel', 'Home Goods', 'Books', 'Tools']
    data = []

    fake.unique.clear()
    for i in range(1, n + 1):
        data.append({
            'product_id': i,
            'product_name': fake.unique.word().capitalize() + " Product",
            'unit_price': round(random.uniform(10.00, 500.00), 2),
            'category': random.choice(categories)
        })

    fake.unique.clear()
    return pd.DataFrame(data)


def generate_orders(n, customer_ids, start_date, end_date):
    logging.info(f"-> Generating {n} orders...")
    orders = []
    order_id_counter = 10000
    statuses = ['Completed', 'Pending', 'Shipped', 'Cancelled']

    for _ in range(n):
        order_id_counter += 1
        order_date = get_random_date(start_date, end_date)

        orders.append({
            'order_id': order_id_counter,
            'customer_id': random.choice(customer_ids),
            'order_date': order_date.strftime('%Y-%m-%d %H:%M:%S'),
            'order_status': random.choices(statuses, weights=[0.6, 0.2, 0.1, 0.1], k=1)[0],
            'shipping_cost': round(random.uniform(5.00, 30.00), 2)
        })

    return pd.DataFrame(orders)


def generate_order_details(order_ids, df_products):
    logging.info(f"-> Generating details for {len(order_ids)} orders...")
    details = []
    price_map = df_products.set_index('product_id')['unit_price'].to_dict()

    for order_id in order_ids:
        num_items = random.randint(1, 5)
        chosen_products = random.sample(df_products['product_id'].tolist(), num_items)

        for product_id in chosen_products:
            quantity = random.randint(1, 4)

            details.append({
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price_at_sale': price_map.get(product_id, 0.00)
            })

    return pd.DataFrame(details)


# ===============================================
# MAIN PIPELINE FUNCTION (INGESTION)
# ===============================================

def run_pipeline():
    logging.info("\n--- STARTING INGESTION PIPELINE (GENERATION AND LOADING) ---")

    try:
        db_connector = MultiDBConnector()
        engine = db_connector.get_engine('web_shop')
        if engine is None:
            logging.error("Database connection engine is None. Aborting pipeline.")
            return
    except Exception as e:
        logging.error(f"FATAL ERROR during connection setup: {e}")
        return

    df_customers = generate_customers(NUM_CUSTOMERS)
    df_products = generate_products(NUM_PRODUCTS)

    customer_ids = df_customers['customer_id'].tolist()
    product_ids = df_products['product_id'].tolist()

    df_orders = generate_orders(NUM_ORDERS, customer_ids, ORDERS_START_DATE, ORDERS_END_DATE)
    order_ids = df_orders['order_id'].tolist()
    df_order_details = generate_order_details(order_ids, df_products)

    df_inventory = pd.DataFrame({
        'product_id': product_ids,
        'stock_quantity': [random.randint(50, 500) for _ in product_ids]
    })

    dataframes_to_load = [
        (df_customers, 'customers'),
        (df_products, 'products'),
        (df_orders, 'orders'),
        (df_inventory, 'inventory'),
        (df_order_details, 'order_details'),
    ]

    for df, table_name in dataframes_to_load:
        logging.info(f"\n-> Loading {len(df)} rows into table '{table_name}'...")
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            logging.info(f"   SUCCESS: Data inserted into table '{table_name}'.")
        except Exception as e:
            logging.error(f"   ERROR loading table '{table_name}': {e}")
            raise

    logging.info("\n--- INGESTION PIPELINE COMPLETED! The Database is Populated. ---")