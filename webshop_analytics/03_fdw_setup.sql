CREATE FOREIGN TABLE orders_foreign (
    order_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE,
    order_status VARCHAR(50) NOT NULL,
    shipping_cost NUMERIC(10, 2)
)
SERVER webshop_server
OPTIONS (
    schema_name 'public',
    table_name 'orders'
);