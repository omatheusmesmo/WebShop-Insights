import numpy as np
import pandas as pd
from datetime import datetime
from .db_connections import MultiDBConnector

CALCULATION_DATE = datetime.now()
TARGET_TABLE = 'financial_summary'

# ===============================================
# 1. EXTRACTION LOGIC (HIGH WATER MARK BY DATE)
# ===============================================

def get_latest_processed_date(target_engine):

    print("INFO: Step E1 - Checking High Water Mark (latest order date) in target database.")

    hwm_query = f"SELECT MAX(order_date) AS max_date FROM {TARGET_TABLE}"

    try:
        df_mark = pd.read_sql(hwm_query, target_engine)
        max_date = df_mark['max_date'].iloc[0]

        if pd.notna(max_date):
            latest_date_str = max_date.strftime('%Y-%m-%d')
            print(f"INFO: High Water Mark (latest order date processed): {latest_date_str}")
            return latest_date_str
        else:
            print("INFO: Target table is empty. Starting from the beginning.")
            return '1900-01-01'

    except Exception as e:
        if "does not exist" in str(e):
            print(f"WARNING: Target table {TARGET_TABLE} does not exist. Starting from the beginning.")
            return '1900-01-01'

    print(f"ERROR: Could not get High Water Mark: {e}")
    return None


# ===============================================
# 2. E/T: INCREMENTAL METRICS CALCULATION (CÁLCULO MISTO)
# ===============================================

def calculate_incremental_metrics(target_engine, latest_date):

    print(f"INFO: Step E/T - Calculating metrics for dates > {latest_date}.")

    financial_metrics_query = f"""
        SELECT
            os.order_date,
            SUM(CASE WHEN ofgn.order_status = 'Completed' THEN 1 ELSE 0 END) AS total_completed_orders,
            SUM(CASE WHEN ofgn.order_status = 'Cancelled' THEN 1 ELSE 0 END) AS total_cancelled_orders,
            SUM(CASE WHEN ofgn.order_status = 'Completed' THEN os.order_subtotal + ofgn.shipping_cost ELSE 0 END) AS total_revenue,
            AVG(CASE WHEN ofgn.order_status = 'Completed' THEN os.order_subtotal + ofgn.shipping_cost ELSE NULL END) AS average_order_value
        FROM
            order_subtotals os 
        JOIN
            orders_foreign ofgn ON ofgn.order_id = os.order_id 
        WHERE
            os.order_date > '{latest_date}' 
        GROUP BY
            os.order_date
        ORDER BY
            os.order_date;
    """

    try:
        df_metrics = pd.read_sql(financial_metrics_query, target_engine)

        if df_metrics.empty:
            print("INFO: No new completed orders found since last run.")
            return None

        df_metrics['total_orders'] = df_metrics['total_cancelled_orders'] + df_metrics['total_completed_orders']

        df_metrics['cancellation_rate'] = np.where(
            df_metrics['total_orders'] > 0,
            df_metrics['total_cancelled_orders'] / df_metrics['total_orders'],
            0.00
        )

        df_metrics = df_metrics.drop(columns=['total_orders'])

        df_metrics['calculation_date'] = CALCULATION_DATE

        print(f"INFO: {len(df_metrics)} new days of metrics calculated and rate computed.")
        return df_metrics

    except Exception as e:
        print(f"FATAL ERROR during metric calculation: {e}")
        return None

# ===============================================
# 3. MAIN PIPELINE EXECUTION (LOAD)
# ===============================================

def run_pipeline():
    print(f"\n--- STARTING PIPELINE: {TARGET_TABLE} Incremental Load ---")

    connector = MultiDBConnector()
    target_engine = connector.get_engine("analytics")

    if target_engine is None:
        print("FATAL: Cannot connect to required database. Exiting.")
        return

    latest_date = get_latest_processed_date(target_engine)
    if latest_date is None:
        return

    df_new_metrics = calculate_incremental_metrics(target_engine, latest_date)
    if df_new_metrics is None or df_new_metrics.empty:
        print("INFO: No new data to process. Pipeline finished.")
        return

    print(f"INFO: Step L - Loading {len(df_new_metrics)} new daily summaries into '{TARGET_TABLE}'.")
    try:
        df_new_metrics.to_sql(
            TARGET_TABLE,
            target_engine,
            if_exists='append',
            index=False,
            method='multi'
        )
        print(f"SUCCESS: Data successfully appended to {TARGET_TABLE}.")

    except Exception as e:
        # Se a tabela não existir, o erro vai ocorrer aqui na primeira execução.
        print(f"FATAL ERROR during data loading: {e}")

    print("--- PIPELINE FINISHED ---")

if __name__ == "__main__":
    run_pipeline()