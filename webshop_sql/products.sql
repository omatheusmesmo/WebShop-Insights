-- Table to store product/item details
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) UNIQUE NOT NULL,
    category VARCHAR(100),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price > 0),
    description TEXT
);