import random
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
from faker import Faker
from sqlalchemy import create_engine

# Inicializa o Faker
fake = Faker('pt_BR')

# ===============================================
# CONFIGURAÇÕES DO BANCO DE DADOS E VOLUME
# ===============================================
# ATENÇÃO: Use uma senha forte no ambiente real!
DB_USER = 'postgres'
DB_PASSWORD = '1234!@#$'  # Altere para a sua senha!
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'web_shop'

# Codifica a senha para uso seguro na URL, resolvendo o problema do '@'
DB_PASSWORD_ENCODED = quote_plus(DB_PASSWORD)

# Volume de Dados a Ser Gerado
NUM_CUSTOMERS = 500
NUM_PRODUCTS = 30
NUM_ORDERS = 5000
ORDERS_START_DATE = datetime(2025, 1, 1)
ORDERS_END_DATE = datetime(2025, 12, 31)


# ===============================================
# 1. FUNÇÕES DE CONEXÃO
# ===============================================

def create_db_engine():
    """Cria a engine de conexão SQLAlchemy, com logging desativado."""

    # Monta a URL usando a senha codificada
    db_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD_ENCODED}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # DEBUG: Mostra a URL de conexão (sem a senha original)
    print(
        f"DEBUG: Tentando conectar com URL (senha codificada): postgresql+psycopg2://{DB_USER}:*****@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    try:
        engine = create_engine(db_url, echo=False)
        print("Conexão com o PostgreSQL estabelecida.")
        return engine
    except Exception as e:
        print(f"ERRO: Não foi possível conectar ao banco de dados: {e}")
        return None


# ===============================================
# 2. FUNÇÕES DE GERAÇÃO DE DADOS (EXTRAÇÃO SIMULADA)
# ===============================================

def get_random_date(start_date, end_date):
    """Gera uma data aleatória entre duas datas."""
    delta = end_date - start_date
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = random.randrange(int_delta)
    return start_date + timedelta(seconds=random_second)


def generate_customers(n):
    """Cria o DataFrame de clientes com unicidade de email garantida."""
    print(f"-> Gerando {n} clientes...")
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
    """Gera o DataFrame de produtos."""
    print(f"-> Gerando {n} produtos...")
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
    """Gera o DataFrame de pedidos transacionais."""
    print(f"-> Gerando {n} pedidos...")
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
    """Gera o DataFrame de detalhes de pedidos (itens dentro dos pedidos)."""
    print(f"-> Gerando detalhes para {len(order_ids)} pedidos...")
    details = []

    # Mapeia preços atuais dos produtos
    price_map = df_products.set_index('product_id')['unit_price'].to_dict()

    for order_id in order_ids:
        # Cada pedido tem entre 1 e 5 itens diferentes
        num_items = random.randint(1, 5)
        # Seleciona uma amostra ÚNICA de produtos para este pedido (evitando violação da PK composta)
        chosen_products = random.sample(df_products['product_id'].tolist(), num_items)

        for product_id in chosen_products:
            # Já não precisamos do detail_id_counter
            quantity = random.randint(1, 4)

            details.append({
                # REMOVEMOS: 'order_detail_id': detail_id_counter,
                'order_id': order_id,
                'product_id': product_id,
                'quantity': quantity,
                'unit_price_at_sale': price_map.get(product_id, 0.00)
            })

    return pd.DataFrame(details)


# ===============================================
# 3. FUNÇÃO PRINCIPAL DO PIPELINE (SOMENTE INGESTÃO)
# ===============================================

def run_pipeline():
    print("\n--- INICIANDO PIPELINE DE INGESTÃO (GERAÇÃO E CARREGAMENTO) ---")

    engine = create_db_engine()
    if engine is None:
        return

    # --- FASE DE EXTRAÇÃO & GERAÇÃO DE DADOS ---
    df_customers = generate_customers(NUM_CUSTOMERS)
    df_products = generate_products(NUM_PRODUCTS)

    customer_ids = df_customers['customer_id'].tolist()
    product_ids = df_products['product_id'].tolist()

    df_orders = generate_orders(NUM_ORDERS, customer_ids, ORDERS_START_DATE, ORDERS_END_DATE)
    order_ids = df_orders['order_id'].tolist()
    df_order_details = generate_order_details(order_ids, df_products)

    # Geração do Inventário
    df_inventory = pd.DataFrame({
        'product_id': product_ids,
        'stock_quantity': [random.randint(50, 500) for _ in product_ids]
    })

    # --- FASE DE CARREGAMENTO (LOAD) ---
    dataframes_to_load = [
        (df_customers, 'customers'),
        (df_products, 'products'),
        (df_orders, 'orders'),
        (df_inventory, 'inventory'),
        (df_order_details, 'order_details'),
    ]

    for df, table_name in dataframes_to_load:
        print(f"\n-> Carregando {len(df)} linhas para a tabela '{table_name}'...")
        try:
            df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
            print(f"   SUCESSO: Dados inseridos na tabela '{table_name}'.")
        except Exception as e:
            print(f"   ERRO ao carregar tabela '{table_name}': {e}")
            return

    print("\n--- PIPELINE DE INGESTÃO CONCLUÍDO! O Banco de Dados está Populado. ---")


if __name__ == "__main__":
    run_pipeline()