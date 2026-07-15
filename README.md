# PySpark Medallion ETL Pipeline

A production-style data engineering and data science project built on:

- **Apache Spark 4.1** + **Delta Lake 4.1** — Medallion architecture (Bronze → Silver → Gold)
- **PostgreSQL** (Neon cloud) — source system
- **scikit-learn 1.5** — ML models (churn, LTV, demand, segmentation)
- **MLflow 2.17** — experiment tracking
- **Apache Airflow 2.10** — pipeline orchestration

---

## Project Structure

```
spark-project/
├── config/
│   └── spark_config.py          # SparkSession + JDBC config
├── etl/
│   ├── bronze/bronze_etl.py     # Raw ingestion from PostgreSQL → Delta
│   ├── silver/silver_etl.py     # Cleaning, deduplication, enrichment
│   └── gold/gold_etl.py         # Business aggregations (5 tables)
├── analysis/
│   ├── business_analytics.py    # Core Spark SQL analytics (RFM, rankings, trends)
│   └── extended_analytics.py   # 12 deep-analytics methods (cohort, ABC, anomalies…)
├── ml/
│   ├── feature_engineering.py  # Build ML feature store from Delta
│   ├── models.py                # 4 models: churn, LTV, demand, segmentation
│   └── ml_pipeline.py           # Orchestrator — runs feature eng + all models
├── dags/
│   └── medallion_pipeline_dag.py  # Airflow DAG (daily at 02:00 UTC)
├── jars/
│   └── postgresql-42.7.4.jar    # PostgreSQL JDBC driver
├── data/                        # Delta Lake files (created at runtime)
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── mlruns/                      # MLflow experiment tracking (created at runtime)
├── sql/                         # Reference SQL scripts (18 files)
├── generate_data.py             # Seed 1 000-row fake data into PostgreSQL
├── main.py                      # Standalone pipeline runner
└── requirements.txt             # Python dependencies
```

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.12 | |
| Java (JRE) | 17 | `sudo apt install openjdk-17-jre-headless` |
| PostgreSQL | cloud / local | Neon credentials in `.env` |

---

## Setup

### 1. Clone and create virtual environment

```bash
git clone <repo-url>
cd spark-project
python3 -m venv venv
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

> The first run downloads Delta Lake JARs (~11 MB) from Maven Central automatically.

### 3. Configure environment variables

Create a `.env` file in the project root (already present if cloned):

```ini
DB_HOST=<your-neon-host>
DB_NAME=neondb
DB_USER=neondb_owner
DB_PASSWORD=<your-password>
DB_PORT=5432
```

### 4. Set JAVA_HOME

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Add to `~/.bashrc` to make it permanent:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

### 5. Seed the database (first time only)

```bash
python generate_data.py
```

Inserts 1 000 rows each into: `customers`, `products`, `employees`, `stores`, `orders`, `sales`.

---

## Running the Pipeline

### Full pipeline (all layers)

```bash
python main.py
```

Runs: Bronze → Silver → Gold → Core Analytics → Extended Analytics → ML

### Run a single layer

```bash
python main.py --layer bronze      # Ingest from PostgreSQL → data/bronze/
python main.py --layer silver      # Clean Bronze → data/silver/
python main.py --layer gold        # Aggregate Silver → data/gold/
python main.py --layer analytics   # Core + Extended Spark SQL analytics
python main.py --layer ml          # Feature engineering + train all 4 models
```

---

## Analytics

### Core Analytics (`analysis/business_analytics.py`)

| Query | Description |
|---|---|
| Monthly revenue by category | Revenue trends per product category |
| Top 10 customers by LTV | Highest lifetime-value customers |
| Product performance ranking | Star / Cash Cow / High Profit / Average |
| Store comparison | Revenue, orders, margin per store |
| Time series + MoM growth | Month-over-month growth by category |
| RFM analysis | Recency / Frequency / Monetary quintile scoring |
| Cohort retention | Customer cohort retention rates |
| Cross-sell analysis | Orders with 2+ products — category co-occurrence |
| Moving averages | 7-day and 30-day revenue moving averages |
| YoY comparison | Year-over-year revenue growth |
| Pareto / ABC | 80-20 product revenue distribution |

### Extended Analytics (`analysis/extended_analytics.py`)

| # | Method | Description |
|---|---|---|
| 1 | Cohort retention matrix | Monthly cohort × months-since-first-purchase retention % |
| 2 | RFM segmentation | 9 named segments: Champions, Loyal, At Risk, Lost… |
| 3 | ABC product classification | A/B/C classes by cumulative revenue share |
| 4 | Basket / cross-sell analysis | Top category co-occurrence pairs in multi-item orders |
| 5 | Revenue trend | Daily revenue with 7d + 30d moving averages |
| 6 | LTV distribution | Percentiles (P10–P95), mean, std dev across all customers |
| 7 | Category × country heatmap | Revenue matrix by product category and store country |
| 8 | Discount effectiveness | Avg order value and profit margin per discount bucket |
| 9 | Day-of-week seasonality | Revenue and unit patterns by weekday and month |
| 10 | Anomaly detection | Z-score flagging of unusual revenue days per category |
| 11 | Employee performance | Orders handled, revenue generated, revenue-to-salary ratio |
| 12 | Inventory risk | Days-of-cover per product — STOCKOUT RISK / OVERSTOCKED / OK |

Run extended analytics standalone:

```bash
python -m analysis.extended_analytics
```

---

## Machine Learning

### Models (`ml/models.py`)

| Model | Algorithm | Target | Key Metrics |
|---|---|---|---|
| Customer Churn | Gradient Boosting (classifier) | `is_churned` (recency > 90 days) | Accuracy, F1, ROC-AUC |
| Customer LTV | Gradient Boosting (regressor) | `lifetime_value` (£) | MAE, RMSE, R² |
| Product Demand | Random Forest (regressor) | `units_sold` per week | MAE, RMSE, R² |
| Customer Segmentation | KMeans (k=3–8, elbow) | Unsupervised | Silhouette score |

Each model:
- Builds a feature matrix from Silver/Gold Delta tables
- Trains with `GridSearchCV` (3-fold CV) to pick best hyperparameters
- Evaluates on a held-out 20% test split
- Logs params, metrics, and the sklearn pipeline artifact to MLflow

### Feature engineering highlights

**Customer features (25 columns)**
- RFM base: recency_days, frequency, monetary
- Statistical: std/min/max order value, avg discount
- Behavioural: distinct_categories, distinct_stores, orders_last_30d, orders_last_90d
- Demographic: gender, tenure_days, preferred_category (one-hot)

**Product features (21 columns)**
- Temporal: week, year, lag_1w, lag_2w, lag_4w, rolling_mean_4w
- Product: cost_price, selling_price, profit_margin, stock
- Encoded: category (6 dummies), price_tier (3 dummies)

### Run ML pipeline standalone

```bash
python main.py --layer ml
```

Or directly:

```bash
python -m ml.ml_pipeline
```

### View results in MLflow UI

```bash
mlflow ui --backend-store-uri ./mlruns
```

Open [http://localhost:5000](http://localhost:5000) in your browser.

Four experiments are tracked:
- `churn_classifier`
- `ltv_regressor`
- `demand_forecaster`
- `customer_segmentation`

---

## Airflow Orchestration

The DAG at `dags/medallion_pipeline_dag.py` runs the full pipeline daily at 02:00 UTC.

### Task dependency graph

```
start
  └─► bronze_ingestion
        └─► silver_processing
              └─► gold_aggregation
                    ├─► analytics
                    └─► ml_pipeline
                          └─► pipeline_complete
```

### Setup Airflow (local standalone)

**1. Initialise the Airflow database and create admin user**

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

airflow db migrate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

**2. Point Airflow at the project DAGs folder**

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

Or add to `airflow_home/airflow.cfg`:

```ini
dags_folder = /home/siddi/spark-project/dags
load_examples = False
```

**3. Start the Airflow webserver and scheduler**

In two separate terminals:

```bash
# Terminal 1 — webserver
export AIRFLOW_HOME=$(pwd)/airflow_home
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
airflow webserver --port 8080
```

```bash
# Terminal 2 — scheduler
export AIRFLOW_HOME=$(pwd)/airflow_home
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
airflow scheduler
```

Open [http://localhost:8080](http://localhost:8080) (admin / admin).

**4. Trigger the DAG manually**

Via UI: enable `medallion_etl_pipeline` → click **Trigger DAG**

Or via CLI:

```bash
airflow dags trigger medallion_etl_pipeline
```

**5. Run the full pipeline from the CLI without the scheduler**

```bash
airflow dags test medallion_etl_pipeline $(date +%Y-%m-%d)
```

### Airflow environment variables

Set these before starting Airflow so each task inherits them:

```bash
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor   # single machine
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PYTHONPATH=$(pwd)                             # so task imports resolve
```

---

## Requirements

Install all dependencies:

```bash
pip install -r requirements.txt
```

Key packages and versions:

| Package | Version |
|---|---|
| pyspark | 4.1.2 |
| delta-spark | 4.1.0 |
| apache-airflow | 2.10.4 |
| scikit-learn | 1.5.2 |
| mlflow | 2.17.2 |
| psycopg2-binary | 2.9.9 |
| python-dotenv | 1.0.0 |
| pandas | 2.0+ |
| faker | (latest) |

---

## Analytics Notebook

`notebooks/full_analytics.ipynb` merges all 23 analytics sections (BusinessAnalytics + ExtendedAnalytics) with rich visuals — Plotly interactive charts, seaborn heatmaps, and matplotlib plots.

**Launch:**
```bash
source venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
jupyter lab notebooks/full_analytics.ipynb
```
Or classic Jupyter:
```bash
jupyter notebook notebooks/full_analytics.ipynb
```

**Sections in the notebook:**
| # | Section | Chart types |
|---|---|---|
| 1 | Setup & Spark Session | — |
| 2 | Monthly Revenue by Category | Plotly grouped bar + stacked area |
| 3 | Revenue Trend & Moving Averages | Plotly multi-line (7d / 30d MA) |
| 4 | Year-over-Year Comparison | Matplotlib grouped bar per category |
| 5 | Top Customers by LTV | Plotly horizontal bar + scatter bubble |
| 6 | Customer LTV Distribution | Histogram + box plot with percentiles |
| 7 | Customer Segment Summary | Plotly subplots + churn risk pie |
| 8 | RFM Segmentation | Bar, bubble chart, funnel |
| 9 | Cohort Retention Matrix | Seaborn annotated heatmap |
| 10 | Product Performance Ranking | Horizontal bar + revenue vs margin scatter |
| 11 | ABC Classification / Pareto | Summary bars + Pareto cumulative curve |
| 12 | Store Performance | Horizontal bar + scatter bubble |
| 13 | Category × Country Heatmap | Seaborn heatmap (top 20 countries) |
| 14 | Basket / Cross-Sell Analysis | Horizontal bar + histogram |
| 15 | Discount Effectiveness | 3-panel bar (order value, profit, margin) |
| 16 | Day-of-Week & Monthly Seasonality | Bar charts + year × month heatmap |
| 17 | Sales Anomaly Detection | Time series per category with anomaly markers |
| 18 | Employee Performance | Horizontal bar + department summary |
| 19 | Inventory Risk | Pie chart + scatter + stockout table |
| 20 | MoM Growth Categories | Plotly faceted bar chart |
| 21 | Advanced Window Analytics | Revenue + MA area chart + distribution |
| 22 | Executive KPI Summary | Printed dashboard table |
| 23 | Cleanup | — |

---

## Quick Reference

```bash
# 0. Activate environment
source venv/bin/activate
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 1. Seed database
python generate_data.py

# 2. Run full pipeline
python main.py

# 3. Run individual layers
python main.py --layer bronze
python main.py --layer silver
python main.py --layer gold
python main.py --layer analytics
python main.py --layer ml

# 4. Extended analytics only
python -m analysis.extended_analytics

# 5. ML pipeline only
python -m ml.ml_pipeline

# 6. MLflow UI
mlflow ui --backend-store-uri ./mlruns
# → http://localhost:5000

# 7. Analytics notebook
jupyter lab notebooks/full_analytics.ipynb
# → http://localhost:8888

# 7. Airflow (one-time setup)
export AIRFLOW_HOME=$(pwd)/airflow_home
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export PYTHONPATH=$(pwd)
airflow db migrate
airflow users create --username admin --firstname Admin \
    --lastname User --role Admin --email admin@example.com --password admin

# 8. Start Airflow services
airflow webserver --port 8080   # terminal 1
airflow scheduler               # terminal 2
# → http://localhost:8080

# 9. Trigger DAG manually
airflow dags trigger medallion_etl_pipeline
```

---

## Data Flow

```
PostgreSQL (Neon)
      │
      │  JDBC
      ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion + audit columns + basic null filters
│  Delta Lake │  data/bronze/{customers,products,employees,stores,orders,sales}
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Deduplication, standardisation, enrichment, date dimensions
│  Delta Lake │  data/silver/{…same tables…}
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Business aggregations
│  Delta Lake │  daily_sales_summary, customer_analytics,
│             │  product_performance, store_performance,
│             │  monthly_time_series
└──────┬──────┘
       │
   ┌───┴───────────────┐
   ▼                   ▼
Analytics           ML Pipeline
(Spark SQL)         (scikit-learn + MLflow)
12 analyses         4 models logged to mlruns/
```
