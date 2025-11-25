import pandas as pd
# Removed unnecessary SQLAlchemy import, as pandas handles it internally with the engine.
from datetime import datetime
from db_connections import MultiDBConnector

# Define the current execution timestamp
CALCULATION_DATE = datetime.now()
TARGET_TABLE = 'order_subtotals'


# ===============================================
# 1. EXTRACTION LOGIC (HIGH WATER MARK)
# ===============================================

def get_high_water_mark(target_engine):

    print("INFO: Step E1 - Checking High Water Mark in target database.")

    high_water_mark_query = f"SELECT MAX(order_id) AS max_id FROM {TARGET_TABLE}"

    try:
        df_mark = pd.read_sql(high_water_mark_query, target_engine)

        max_id = df_mark['max_id'].iloc[0]

        high_water_mark = int(max_id) if pd.notna(max_id) else 0

        print(f"INFO: High Water Mark (MAX Order ID processed): {high_water_mark}")
        return high_water_mark

    except Exception as e:
        # For robustness, we catch errors if the table doesn't exist yet (which is common on first run).
        if "does not exist" in str(e):
            print(f"WARNING: Target table {TARGET_TABLE} does not exist. Starting load from 0.")
            return 0
        print(f"ERROR: Could not get High Water Mark: {e}")
        return None


# ===============================================
# 2. E/T: INCREMENTAL SUBTOTAL CALCULATION (ADJUSTED)
# ===============================================

def calculate_subtotals_incremental(source_engine, high_water_mark):
    """
    Extracts new order line items, joins with 'orders' to get the date, and calculates the subtotal.
    """
    print("INFO: Step E2/T - Calculating incremental order subtotals.")

    # ADJUSTED QUERY: Includes JOIN with 'orders' and uses o.order_date in SELECT and GROUP BY.
    subtotal_query = f"""
    SELECT
        od.order_id,
        SUM(od.quantity * od.unit_price_at_sale) AS order_subtotal,
        CAST(o.order_date AS DATE) AS order_date -- Extracting the transaction date
    FROM
        order_details od 
    JOIN
        orders o ON o.order_id = od.order_id -- Join to get the order date
    WHERE
        od.order_id > {high_water_mark}
    GROUP BY
        od.order_id,
        o.order_date -- Necessary for grouping when including order_date
    ORDER BY
        od.order_id;
    """

    try:
        # Read the aggregated data directly into a DataFrame
        df_subtotals = pd.read_sql(subtotal_query, source_engine)

        # Add the metadata column required by the target DDL
        df_subtotals['calculation_date'] = CALCULATION_DATE

        print(f"INFO: {len(df_subtotals)} new order subtotals calculated.")
        return df_subtotals

    except Exception as e:
        print(f"ERROR during incremental subtotal calculation: {e}")
        return None


# ===============================================
# 3. MAIN PIPELINE EXECUTION (LOAD)
# ===============================================

def run_pipeline():
    """Orchestrates the incremental load of order subtotals."""
    print(f"\n--- STARTING PIPELINE: {TARGET_TABLE} Incremental Load ---")

    connector = MultiDBConnector()

    # Connect to both databases
    source_engine = connector.get_engine("web_shop")
    target_engine = connector.get_engine("analytics")

    if source_engine is None or target_engine is None:
        print("FATAL: Cannot connect to required databases. Exiting.")
        return

    # 1. E1: Get High Water Mark
    hwm = get_high_water_mark(target_engine)
    if hwm is None:
        return

    # 2. E2/T: Calculate Incremental Subtotals
    df_new_subtotals = calculate_subtotals_incremental(source_engine, hwm)
    if df_new_subtotals is None or df_new_subtotals.empty:
        print("INFO: No new orders to process. Pipeline finished.")
        return

    # 3. L: Load New Subtotals to Analytics DB
    print(f"INFO: Step L - Loading {len(df_new_subtotals)} new subtotals into '{TARGET_TABLE}'.")
    try:
        # Load the data. Since we are using an incremental strategy, we always APPEND.
        df_new_subtotals.to_sql(
            TARGET_TABLE,
            target_engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"SUCCESS: Data successfully loaded into {TARGET_TABLE}.")

    except Exception as e:
        print(f"FATAL ERROR during data loading into {TARGET_TABLE}: {e}")

    print("--- PIPELINE FINISHED ---")


if __name__ == "__main__":
    run_pipeline()