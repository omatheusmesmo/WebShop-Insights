-- Table to store consolidated financial metrics (AOV, Total Revenue, etc.).
CREATE TABLE financial_summary (
    -- Unique identifier for each metrics calculation record.
    summary_id SERIAL PRIMARY KEY,

    -- Metadata related to the execution of the ETL pipeline.
    calculation_date TIMESTAMP NOT NULL,

    -- Period covered by this summary (if applicable, though typically transactional data is processed).
    start_date DATE,
    end_date DATE,

    -- Metrics related to order volume
    total_completed_orders INTEGER NOT NULL CHECK (total_completed_orders >= 0),
    total_cancelled_orders INTEGER NOT NULL CHECK (total_cancelled_orders >= 0),

    -- Primary financial metrics
    total_revenue NUMERIC(18, 2) NOT NULL CHECK (total_revenue >= 0),
    average_order_value NUMERIC(18, 2) NOT NULL CHECK (average_order_value >= 0),

    -- Quality metric (cancellations as a percentage of total orders)
    cancellation_rate NUMERIC(5, 4) NOT NULL CHECK (cancellation_rate >= 0 AND cancellation_rate <= 1),

    -- Ensures we don't calculate the same summary twice for the same period start/end dates.
    UNIQUE (start_date, end_date)
);