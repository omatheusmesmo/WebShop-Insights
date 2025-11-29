-- Table to store the pre-calculated subtotal for each order.
CREATE TABLE order_subtotals (
    order_id INTEGER PRIMARY KEY,
    order_date DATE NOT NULL,
    order_subtotal NUMERIC(18, 2) NOT NULL CHECK (order_subtotal >= 0),
    calculation_date TIMESTAMP NOT NULL
);