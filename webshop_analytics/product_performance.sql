CREATE TABLE product_performance (
    performance_id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    aggregation_day DATE NOT NULL,
    total_units_sold INTEGER NOT NULL CHECK (total_units_sold >= 0),
    total_revenue NUMERIC(18, 2) NOT NULL CHECK (total_revenue >= 0),
    revenue_rank INTEGER,
    sales_velocity_score NUMERIC(5, 2),
    calculation_date TIMESTAMP NOT NULL,

    UNIQUE (product_id, aggregation_day)
);