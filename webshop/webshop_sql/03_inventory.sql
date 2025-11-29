-- Table for stock control
CREATE TABLE inventory (
    product_id INTEGER PRIMARY KEY,
    stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    warehouse_location VARCHAR(100),
    last_updated TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key link to products
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);