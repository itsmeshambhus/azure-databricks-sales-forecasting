# Predictive Analysis and Visualization for Forecasting Sales and Customer Behavior

This project presents a full-stack data analytics pipeline designed to forecast future sales and analyze customer behavior using Azure Databricks, Azure Synapse, and Tableau. The solution follows modern data engineering practices including ETL, star schema modeling, and interactive BI dashboards.

---

## 🚀 Project Highlights

- 📊 Performed **Exploratory Data Analysis (EDA)** on sales and customer datasets in **Azure Databricks**
- 🧱 Designed a **Star Schema** for a data warehouse to support efficient querying and reporting
- 🔄 Developed an **ETL pipeline** with:
  - Staging layer
  - Separation of **good and bad data**
  - Data cleansing and transformation
  - Population of a **Fact Table**
- 🔗 Integrated with **Azure Synapse Analytics** to create view layers
- 📈 Built **Tableau Dashboards** connected to Azure for real-time data insights:
  1. Product and Order Performance
  2. Sales Profit
  3. Forecasted Sales Visualization
  4. Forecast sales Details
  5. Forecasted Sales by Location (Map-based)

---

## 🧠 Technologies Used

| Tool / Tech            | Purpose                                      |
|------------------------|----------------------------------------------|
| **Azure Databricks**   | Data analysis, ETL, fact table creation      |
| **Apache Spark**       | Distributed processing with PySpark & SQL    |
| **Azure Synapse**      | SQL views and data warehouse connectivity    |
| **Tableau**            | Interactive dashboarding and visualization   |
| **SQL & Python**       | Data cleaning, transformation, forecasting   |

---

---

## 📊 Dashboards Overview

### 1. **Past Data Visualization**
- Visual summary of historical sales trends
- Metrics by category, region, and time period

### 2. **Forecasted Sales Visualization**
- Predictive analysis using time-series models (e.g., ARIMA)
- Trend line forecasts with confidence intervals

### 3. **Forecasted Sales with Map**
- Geo-visualization of sales predictions
- Interactive map view by region/store location

---

## 📈 Methodology

### ✅ Step-by-Step Process:

1. **Data Ingestion** into Databricks
2. **EDA & Cleaning** (missing values, outliers)
3. **Star Schema Design**:
   - Dimension tables (e.g., Date, Product, Customer)
   - Fact table for Sales
4. **ETL Pipeline**:
   - Staging → Good/Bad data separation
   - Bad data transformed into good format
   - Load into fact and dimension tables
5. **Fact View Creation** on **Azure Synapse**
6. **Tableau Dashboard** connected to Synapse

---

## 🧪 Models Used

- **Time Series Models** for Forecasting:
  - ARIMA / SARIMA / Holt-Winters (Databricks)
- Future forecasts exported to Synapse and visualized in Tableau

---

## 📊 Tableau Dashboards

The following interactive dashboards were developed in Tableau and are published on Tableau Public. They are connected to Azure Synapse (or use extracted data) and visualize both historical and forecasted sales data:

---

### 🔹 1. [Sales and Profit Overview](https://public.tableau.com/app/profile/shambhu.prasad.sah/viz/SalesProfit_17467222389180/Dashboard1)
- Overview of historical sales and profit performance.
- Breakdown by category, segment, region, and time.
- Helps identify high-performing and low-performing segments.

---

### 🔹 2. [Forecasted Sales – Overview](https://public.tableau.com/app/profile/shambhu.prasad.sah/viz/Forecastsales_17469511835060/Dashboard2)
- Forecasted monthly sales using time-series analysis.
- Clear trend visualization with predictive modeling.
- Confidence intervals and expected future values included.

---

### 🔹 3. [Forecast by City (Map-Based)](https://public.tableau.com/app/profile/shambhu.prasad.sah/viz/ForecastBycity/Dashboard1)
- Geo-spatial visualization of future sales by city.
- Interactive map shows expected growth by location.
- Useful for strategic regional planning.

---

### 🔹 4. [Forecast Sales Details](https://public.tableau.com/app/profile/shambhu.prasad.sah/viz/ForecastsalesDetails/Dashboard1)
- Detailed forecast analysis by product, category, and region.
- Drill-down functionality for deeper insights.

---

### 🔹 5. [Product and Order Performance](https://public.tableau.com/app/profile/shambhu.prasad.sah/viz/ProductandOrderPerformance/Dashboard3)
- Analysis of product-wise performance and order distribution.
- Identifies top-selling items and order trends over time.

---

🟢 **All dashboards are hosted publicly on Tableau and accessible without login.**
---

## 🙋‍♂️ Author

**Shambhu Prasad Sah**  
Connect on [LinkedIn](https://www.linkedin.com/in/sahshambhu/)  
