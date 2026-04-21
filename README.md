# 🧾 Customer & Order Data Processing — PySpark + Databricks

![PySpark](https://img.shields.io/badge/PySpark-Apache_Spark-E25A1C?style=flat-square&logo=apachespark)
![Databricks](https://img.shields.io/badge/Databricks-Notebook-FF3621?style=flat-square&logo=databricks)
![Parquet](https://img.shields.io/badge/Output-Parquet-blue?style=flat-square)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat-square)

> A PySpark data engineering pipeline on Databricks that cleanses, enriches, and analyzes customer and order datasets — using Window functions, joins, aggregations, and Parquet output.

---

## 📌 What This Project Does

Processes raw customer and order CSVs through a multi-stage pipeline — handling nulls, type casting, feature extraction, and business analytics like customer ranking, spending behavior, and monthly order trends.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| Apache Spark (PySpark) | Distributed data processing |
| Databricks Notebooks | Execution environment |
| CSV | Input format |
| Parquet | Optimized output format |
| Window Functions | Ranking & trend analysis |

---

## 📁 Dataset

| File | Description |
|------|-------------|
| `customers.csv` | Customer details — city, state, country, registration date, active status |
| `orders.csv` | Order records — customer ID, status, date, amount |

---

## ⚙️ Pipeline Overview

```
CSV Input → Null Handling → Type Casting → Feature Extraction
         → Aggregations → Window Functions → Joins → Parquet Output
```

### 👤 Customer Data Processing
- Load via SparkSession, read CSV
- Null handling on `city`, `state`, `country`
- Cast `registration_date` → DateType, `is_active` → Boolean
- Extract `registration_year`, `registration_month`
- Count distinct cities, states, countries
- Pivot tables on `is_active` grouped by state and country
- Window functions — `rank()`, `dense_rank()`, `row_number()` per state by registration date
- Filter recent customers (`registration_date >= 2023-07-01`)
- Min/max registration dates per city
- Save as Parquet → `/FileStore/tables/processed_customers`

### 📦 Order Data Analysis
- Load `orders.csv`, join with customer data on `customer_id`
- Aggregations:
  - Total orders per customer
  - Total and average spend per customer
  - Order count by status
  - Monthly order trends
- Window functions — rank customers by total spending + order frequency

---

## 📊 Key Insights Generated

| Insight | Method |
|---------|--------|
| Top spenders | Window rank on total spend |
| Most frequent customers | Order count aggregation |
| Monthly order trends | Date truncation + groupBy |
| Active vs Inactive per state | Pivot table |
| Spending vs frequency mapping | Join + aggregation |

---

## 📦 Output

| Output | Location |
|--------|----------|
| Processed customers | `/FileStore/tables/processed_customers` (Parquet) |
| Analytical DataFrames | `customer_total_spend`, `orders_status_count`, etc. |

---

## 📂 Project Structure

```
Mini-project--Read-And-Process/
├── customers.csv              # Customer dataset
├── orders.csv                 # Orders dataset
├── customer_order_pipeline.ipynb   # Databricks notebook
└── README.md
```

---

## 🚀 How to Run

1. Upload `customers.csv` and `orders.csv` to Databricks FileStore
2. Import the notebook into your Databricks workspace
3. Attach to a cluster (Spark 3.x+)
4. Run all cells top to bottom

---

## 🚀 Roadmap

- [ ] Add streaming ingestion via Spark Structured Streaming
- [ ] Connect to Delta Lake for ACID transactions
- [ ] Build dashboard on top of aggregated Parquet output

---

## 📄 License

MIT License — open source and free to use.

---

<p align="center">Built by <a href="https://github.com/AshishChaubey2003">Ashish Kumar Chaubey</a> — B.Tech CSE 2025 | Lucknow, India</p>
<p align="center">
  <a href="https://www.linkedin.com/in/ashishchaubey2dec/">LinkedIn</a> •
  <a href="mailto:sashishchaubey1234@gmail.com">Email</a>
</p>
