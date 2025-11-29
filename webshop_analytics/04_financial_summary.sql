CREATE TABLE financial_summary (
    summary_id SERIAL PRIMARY KEY,
    calculation_date TIMESTAMP NOT NULL,
    order_date DATE NOT NULL UNIQUE,
    total_completed_orders INTEGER NOT NULL CHECK (total_completed_orders >= 0),
    total_cancelled_orders INTEGER NOT NULL CHECK (total_cancelled_orders >= 0),
    total_revenue NUMERIC(18, 2) NOT NULL CHECK (total_revenue >= 0),
    average_order_value NUMERIC(18, 2) NOT NULL CHECK (average_order_value >= 0),
    cancellation_rate NUMERIC(5, 4) NOT NULL CHECK (cancellation_rate >= 0 AND cancellation_rate <= 1)
);