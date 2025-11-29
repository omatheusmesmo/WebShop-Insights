-- FDW SETUP
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER webshop_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host 'localhost',
        port '5432',
        dbname 'web_shop'
    );

CREATE USER MAPPING FOR postgres
    SERVER webshop_server
    OPTIONS (
        user 'postgres',
        password ''
    );

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