-- Table to store detailed metrics per product, including sales volume and rank.
CREATE TABLE product_performance (
    -- Unique identifier for the combination of product and calculation date.
    product_performance_id SERIAL PRIMARY KEY,

    -- Key to join back to the 'products' table in the source database (or a dimension table).
    product_id INTEGER NOT NULL,
    product_name VARCHAR(255) NOT NULL,

    -- Metadata related to the execution time.
    calculation_date TIMESTAMP NOT NULL,

    -- Performance Metrics
    units_sold INTEGER NOT NULL CHECK (units_sold >= 0),
    revenue_by_product NUMERIC(18, 2) NOT NULL CHECK (revenue_by_product >= 0),

    -- Key Analytical Metric: Sales Rank (calculated using a Window Function in the pipeline).
    sales_rank INTEGER NOT NULL CHECK (sales_rank >= 1),

    -- Constraint to prevent duplicate entries for the same product calculated at the same time.
    UNIQUE (product_id, calculation_date)
);