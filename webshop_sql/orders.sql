-- Table to store order header information
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    order_status VARCHAR(50) NOT NULL, -- E.g., 'Pending', 'Completed', 'Cancelled'
    shipping_cost NUMERIC(10, 2) DEFAULT 0.00,

    -- Foreign Key to link order to a customer
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);