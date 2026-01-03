# WebShop-Insights

## Project overview

WebShop-Insights is a small analytics and orchestration project that demonstrates running Apache Airflow DAGs (Python) and batch Java pipelines together on a local Kubernetes environment (Minikube). It includes infrastructure manifests to run Airflow and supporting PostgreSQL databases, example DAGs, SQL schema files for a sample webshop, and both Python and Java pipeline code.

## Contents
- `infra/` - Kubernetes manifests and kustomization for deploying Airflow and databases to Minikube.
- `dags/` - Airflow DAG definitions (Python).
- `pipelines/python/` - Python helper scripts and ETL routines (DB connectors, ETL scripts).
- `pipelines/java/product-ranking-batch/` - Java batch pipeline (Maven project) to compute product rankings.
- `webshop/webshop_sql/` - SQL schema files used to initialize the sample application database.
- `webshop_analytics/` - SQL queries and FDW setup scripts for analytics and reporting.
- `Dockerfile` - Custom Airflow image used by `up_infra.sh`.
- `up_infra.sh` - Automated script to build images, deploy infra to Minikube, initialize DBs and sync DAGs.

## Quickstart (local development with Minikube)

### Prerequisites
- minikube (tested with a recent release)
- kubectl
- Docker (or a working Docker driver used by Minikube)
- Java (for building the Java pipeline locally) and Maven (to build the Java project)
- Python 3.10+ (for running local Python scripts)

### Basic steps
1. Make sure you are at the project root and make the infra script executable:

```bash
chmod +x up_infra.sh
```

2. Create a `.env` file in the project root (if not present) providing the environment variables used by `up_infra.sh` for database credentials and secrets. See `.env.example` below or inspect `up_infra.sh` for expected variables.

3. Run the setup script to start Minikube, build the custom Airflow image, apply Kubernetes manifests, initialize databases, and copy DAGs into the Airflow volume:

```bash
./up_infra.sh
```

4. When the script completes it will print the Airflow UI URL (typically `http://<minikube-ip>:<NodePort>`). Default credentials created by the initialization job are `admin` / `admin`.

## Notes about the custom Airflow image
- The project builds a custom image named `webshop-airflow-custom:latest` (see `Dockerfile`). The image should include Java (OpenJDK) if you intend to execute Java code from inside Airflow containers (for example using `BashOperator`). Alternatively, prefer `KubernetesPodOperator` and run Java apps in a separate Java-specific image.

## Airflow DAGs and synchronization
- DAGs live in `dags/` and are copied into the Airflow pod volume by `up_infra.sh` during setup.
- To apply DAG changes during development you can either re-run `./up_infra.sh` or copy individual DAG files into the running Airflow volume using `kubectl cp`.

## .env.example (variables expected)
Below is an example list of the environment variables that `up_infra.sh` and the Kubernetes manifests expect. Do NOT add sensitive values here — create a local `.env` file with real values on your machine.

```dotenv
# Application (web shop) database connection (used to initialize app data)
WEB_SHOP_DB_HOST=
WEB_SHOP_DB_NAME=
WEB_SHOP_DB_USER=
WEB_SHOP_DB_PASSWORD=

# Analytics database connection (used by ETL and analytics jobs)
ANALYTICS_DB_HOST=
ANALYTICS_DB_NAME=
ANALYTICS_DB_USER=
ANALYTICS_DB_PASSWORD=

# Optional: UID to run the Airflow services as (keeps file ownership predictable)
AIRFLOW_UID=

# Note: other variables referenced by kustomize manifests include:
# POSTGRES_USER, POSTGRES_DB (these are typically provided by the manifests or defaulted in the YAMLs)
```

## Metrics & Business Value
- This project computes a small set of analytics metrics that are useful for day-to-day business decisions. Below are the primary metrics, where they are produced, how often they are updated, and typical business use cases.

### Primary metrics produced

#### 1) Daily Order Subtotals (`order_subtotals`)
- Source: `pipelines/python/order_subtotals.py` (triggered by DAG `order_subtotals_etl`)
- Target table: `order_subtotals` (analytics DB)
- Schedule: daily (Airflow DAG scheduled `@daily`)
- Columns: `order_id`, `order_subtotal`, `order_date`, `calculation_date`
- Business value:
  - Source data for downstream aggregates (daily revenue, AOV, product-level aggregation).
  - Supports order-level reconciliation and validation.
  - Useful for finance close, per-day OLAP, and identifying anomalous orders.

#### 2) Financial Summary (`financial_summary`)
- Source: `pipelines/python/financial_metrics.py` (triggered by DAG `financial_metrics_etl`)
- Target table: `financial_summary` (analytics DB)
- Schedule: daily (Airflow DAG scheduled `@daily`)
- Columns: `summary_id`, `calculation_date`, `order_date`, `total_completed_orders`, `total_cancelled_orders`, `total_revenue`, `average_order_value`, `cancellation_rate`
- Business value:
  - Daily revenue tracking (total_revenue) and trend analysis.
  - Average Order Value (AOV) helps measure customer spend and the impact of promotions or price changes.
  - Cancellation rate is a direct indicator of service or fulfillment issues; an increasing trend may trigger operations investigations.
  - Useful for KPI dashboards (daily/weekly/monthly revenue, AOV, cancellations) and for feeding forecasting models.

#### 3) Product Performance / Ranking (`product_performance`)
- Source: `pipelines/java/product-ranking-batch/` (triggered by DAG `product_ranking_batch_dag` via `KubernetesPodOperator`)
- Target table: `product_performance` (analytics DB)
- Schedule: ad-hoc / batch (DAG schedule is `None` by default; run on demand or via CI/CD)
- Columns: `performance_id`, `product_id`, `aggregation_day`, `total_units_sold`, `total_revenue`, `revenue_rank`, `sales_velocity_score`, `calculation_date`
- Business value:
  - Ranks products by revenue and sales velocity to identify top-sellers and fast-moving SKUs.
  - Informs assortment, reordering, and inventory allocation decisions.
  - Enables targeted promotions for slow-moving items and upsell/cross-sell planning for high-value products.

### How metrics are generated and persisted
- The Python DAGs run daily and use incremental (high-water-mark) logic to process only new rows and append results into analytics tables.
- The Java batch computes product-level aggregations and upserts results into `product_performance` (ON CONFLICT upsert behavior) so the latest aggregates are persisted.
- All analytics tables live in the `analytics` PostgreSQL database deployed by the infra manifests and are populated by the ETL pipelines.

### Suggested example queries and dashboards
- Daily revenue trend (last 30 days):

```sql
SELECT order_date, SUM(total_revenue) AS daily_revenue
FROM financial_summary
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY order_date
ORDER BY order_date;
```

- Top 10 products by revenue for the last 7 days:

```sql
SELECT product_id, SUM(total_revenue) AS revenue_last_7_days
FROM product_performance
WHERE aggregation_day >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY product_id
ORDER BY revenue_last_7_days DESC
LIMIT 10;
```

- Cancellation rate alert (example):
  - Monitor `cancellation_rate` in `financial_summary`. If a 7-day rolling average exceeds a threshold (for example 0.05), trigger an operational investigation.

## Python pipelines (local run)
- The Python helper scripts and ETL examples are in `pipelines/python/`.
- To run locally:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipelines/python/insert_webshop.py   # or other scripts under pipelines/python
```

## Java pipeline (build and run)
- Java batch pipeline source: `pipelines/java/product-ranking-batch/` (Maven project).
- To build locally and create a JAR:

```bash
cd pipelines/java/product-ranking-batch
./mvnw -DskipTests package
# the JAR will be in target/ after a successful build
```

- In Airflow you can run the Java batch using either:
  - `KubernetesPodOperator` with a Java app image that contains the JAR; or
  - `BashOperator` if the `webshop-airflow-custom:latest` image includes Java and the JAR.

## Database schema and analytics SQL
- `webshop/webshop_sql/` contains the SQL files used to initialize the application database (customers, products, inventory, orders, order details).
- `webshop_analytics/` contains FDW setup scripts and reporting queries (financial summary, order subtotals, product performance).

## Troubleshooting
- If the infra script fails, inspect `up_infra.sh` output and use kubectl to check pod states and logs:

```bash
kubectl get pods -n default
kubectl describe pod <pod-name> -n default
kubectl logs <pod-name> -n default
```

## Developer notes & next steps
- Re-run `./up_infra.sh` when you need a fresh environment or to re-sync DAGs.
- Use `kubectl cp` to push specific DAGs during development if you prefer a faster loop than re-running the whole script.
- If you add Java tasks, prefer building a small Java image for the `KubernetesPodOperator` to avoid bundling many runtimes into the Airflow image.

## License & Contact
- No license file is included. Add a LICENSE if you intend to open-source this repository.
- For questions or contribution notes, open an issue or contact the repository owner/maintainer.

--
This README replaces the previous placeholder header and summarizes repository layout, quickstart instructions, and developer pointers for this Airflow + pipelines project.
