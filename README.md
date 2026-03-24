Vendor Sales Analysis — Data Analyst Project

> **End-to-end data pipeline and analytical dashboard to identify top-performing vendors, optimize procurement spend, and surface profitability insights from inventory & sales data.**

---

Table of Contents

- [Project Overview](#-project-overview)
- [Business Problem](#-business-problem)
- [Tech Stack](#-tech-stack)
- [Project Architecture](#-project-architecture)
- [Repository Structure](#-repository-structure)
- [Key Analyses Performed](#-key-analyses-performed)
- [Derived Metrics](#-derived-metrics)
- [Power BI Dashboard](#-power-bi-dashboard)
- [How to Run](#-how-to-run)
- [Key Insights](#-key-insights)
- [Skills Demonstrated](#-skills-demonstrated)

---

## Project Overview

This project performs a comprehensive **Vendor Sales Analysis** for a retail/inventory business. Raw transactional data spread across multiple tables (purchases, sales, invoices, and pricing) is ingested into a **SQLite database**, transformed using **Python and SQL**, and visualized through an interactive **Power BI dashboard**.

The pipeline covers the full analytics lifecycle:

```
Raw CSV Data → Data Ingestion → SQL Transformations → EDA → Vendor Summary → Power BI Dashboard
```

---

## 💼 Business Problem

Companies working with multiple vendors often struggle to answer:

- Which vendors generate the **highest gross profit**?
- Are we **overpaying** on purchases relative to the actual market price?
- Which products have **low sales-to-purchase ratios**, indicating dead stock?
- How does **freight cost** impact overall vendor profitability?

This project builds a data model and analytical layer to answer all of the above.

---

## 🛠️ Tech Stack

| Tool | Purpose |
|------|---------|
| **Python** (pandas, sqlite3) | Data ingestion, transformation, EDA |
| **SQL / SQLite** | Relational data model, complex CTEs |
| **Jupyter Notebook** | Exploratory data analysis, SQL performance testing |
| **Power BI** | Interactive dashboard & business reporting |
| **Logging** | Pipeline monitoring and audit trail |

---

## 🏗️ Project Architecture

```
┌────────────────────┐
│   Raw Data (CSV)   │
└────────┬───────────┘
         │  ingestion.py
         ▼
┌────────────────────┐
│  SQLite Database   │  ← purchases, sales, vendor_invoice,
│   (inventory.db)   │     purchase_prices tables
└────────┬───────────┘
         │  get_vendor_summary.py (SQL CTEs)
         ▼
┌──────────────────────────┐
│  vendor_sales_summary    │  ← Merged, cleaned, enriched table
│  (Derived Metrics Table) │
└────────┬─────────────────┘
         │
    ┌────┴────┐
    ▼         ▼
  EDA      Power BI
(Python)  Dashboard
```

---

## 📁 Repository Structure

```
Data-Analyst-Project/
│
├── ingestion.py                    # Loads CSV data into SQLite database
├── get_vendor_summary.py           # SQL CTEs + data cleaning + derived metrics
├── exploratory data analysis.py    # Standalone EDA script
├── Exploratory_Data_Analysis.ipynb # Detailed EDA with visualizations
├── sql_performance.ipynb           # SQL query profiling & optimization
├── vendor selection.ipynb          # Vendor shortlisting & analysis
├── company project.pbix            # Power BI dashboard file
└── ingestion_db.log                # Pipeline execution log
```

---

## 📐 Key Analyses Performed

### 1. Data Ingestion (`ingestion.py`)
- Reads raw CSV files into a **SQLite relational database**
- Structured into normalized tables: `purchases`, `sales`, `vendor_invoice`, `purchase_prices`
- Logs all operations with timestamps for pipeline traceability

### 2. Vendor Summary Generation (`get_vendor_summary.py`)
Executes a **multi-CTE SQL query** to join all relevant tables:

```sql
WITH FreightSummary AS (
    SELECT VendorNumber, SUM(Freight) AS FreightCost
    FROM vendor_invoice GROUP BY VendorNumber
),
PurchaseSummary AS (
    SELECT p.VendorNumber, p.VendorName, p.Brand,
           SUM(p.Quantity) AS TotalPurchaseQuantity,
           SUM(p.Dollars) AS TotalPurchaseDollars
    FROM purchases p
    JOIN purchase_prices pp ON p.Brand = pp.Brand
    WHERE p.PurchasePrice > 0
    GROUP BY p.VendorNumber, p.VendorName, p.Brand
),
SalesSummary AS (
    SELECT VendorNo, Brand,
           SUM(SalesQuantity) AS TotalSalesQuantity,
           SUM(SalesDollars)  AS TotalSalesDollars
    FROM Sales GROUP BY VendorNo, Brand
)
SELECT ... FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC
```

### 3. Exploratory Data Analysis (`Exploratory_Data_Analysis.ipynb`)
- Distribution analysis of purchase vs. sales volumes
- Outlier detection in pricing and quantity fields
- Correlation heatmaps across numerical features
- Vendor-level aggregations and ranking

### 4. SQL Performance Testing (`sql_performance.ipynb`)
- Profiled query execution times before and after optimization
- Evaluated indexing strategies on large join operations

---

## 📊 Derived Metrics

The `vendor_sales_summary` table adds four business-critical calculated columns:

| Metric | Formula | Business Meaning |
|--------|---------|-----------------|
| **Gross Profit** | `TotalSalesDollars − TotalPurchaseDollars` | Absolute profit per vendor/brand |
| **Profit Margin (%)** | `(GrossProfit / TotalPurchaseDollars) × 100` | Efficiency of procurement spend |
| **Stock Turnover** | `TotalSalesQuantity / TotalPurchaseQuantity` | How fast inventory is being sold |
| **Sales-to-Purchase Ratio** | `TotalSalesDollars / TotalPurchaseDollars` | Revenue generated per dollar spent |

---

## 📈 Power BI Dashboard

The `company project.pbix` file contains an interactive dashboard covering:

- **Top Vendors by Revenue & Profit**
- **Purchase vs. Sales Dollar Comparison** by brand
- **Freight Cost Impact** per vendor
- **Profit Margin distribution** across product categories
- **Stock Turnover heatmap** to identify slow-moving inventory

> 📥 Download the `.pbix` file and open with Power BI Desktop to explore the full interactive report.

---

## ▶️ How to Run

### Prerequisites
```bash
pip install pandas sqlite3 matplotlib seaborn jupyter
```

### Steps

**1. Ingest raw data into the database:**
```bash
python ingestion.py
```

**2. Generate the vendor summary table:**
```bash
python get_vendor_summary.py
```

**3. Run EDA notebooks:**
```bash
jupyter notebook Exploratory_Data_Analysis.ipynb
```

**4. Open the Power BI dashboard:**
- Open `company project.pbix` in Power BI Desktop

---

## 💡 Key Insights

- Vendors with **high purchase volumes but low profit margins** were identified as candidates for renegotiation.
- A subset of brands showed **Stock Turnover < 0.5**, indicating overordering relative to demand.
- **Freight costs** for certain vendors significantly eroded their apparent profitability.
- The top 10 vendors by purchase dollars contributed disproportionately to overall gross profit.

---

## 🧠 Skills Demonstrated

- ✅ **SQL** — Multi-table JOINs, Window Functions, CTEs, aggregations
- ✅ **Python** — pandas data wrangling, data type handling, null treatment
- ✅ **Data Pipeline Design** — Ingestion → Transform → Load → Visualize
- ✅ **Power BI** — Data modeling, DAX metrics, interactive reporting
- ✅ **EDA** — Statistical profiling, outlier detection, correlation analysis
- ✅ **Software Engineering Practices** — Logging, modular functions, reusable scripts

---

## 👤 Author

**Harsh** — [GitHub Profile](https://github.com/harsh23421)

---

*This project was built to demonstrate end-to-end data analytics skills including data engineering, SQL, Python, and business intelligence reporting.*
