-- Table to store line-item details (many-to-many relationship)
CREATE TABLE order_details (
    order_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_at_sale NUMERIC(10, 2) NOT NULL CHECK (unit_price_at_sale >= 0),

    -- Composite Primary Key (an order cannot have the same product twice in the details)
    PRIMARY KEY (order_id, product_id),

    -- Foreign Keys to link details to the order and the product
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);