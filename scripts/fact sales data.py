# Databricks notebook source

# COMMAND ----------

fact_sales = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/fact_sales')


# COMMAND ----------

df = spark.read.format('parquet').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/silver')


# COMMAND ----------

dim_date = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_date')
dim_product = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_product')
dim_order = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_order')
dim_location = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_location')
dim_customer = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_customer')



# COMMAND ----------

dim_location.count()

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

from pyspark.sql.functions import col

# Alias the original data
df_main = df.alias("f")

# Read dimension tables
dim_customer = spark.table("DimCustomer").alias("dc")
dim_product = spark.table("DimProduct").alias("dp")
dim_location = spark.table("DimLocation").alias("dl")
dim_order = spark.table("DimOrder").alias("do")
dim_date = spark.table("DimDate").alias("dd")

fact_sales = df.alias("f") \
    .join(dim_order.select("Order_ID", "Order_Key"), on="Order_ID", how="inner") \
    .join(dim_customer.select("Customer_ID", "Customer_Key"), on="Customer_ID", how="inner") \
    .join(dim_product.select("Product_ID", "Product_Key"), on="Product_ID", how="inner") \
    .join(dim_location.select("Country", "State", "City", "Location_Key"),
          on=["Country", "State", "City"], how="inner") \
    .join(dim_date_order.select(F.col("Order_Date").alias("Order_Date_Value"), F.col("Date_Key").alias("Order_Date_Key")),
          F.col("f.Order_Date") == F.col("Order_Date_Value"), "inner") \
    .join(dim_date_ship.select(F.col("Order_Date").alias("Ship_Date_Value"), F.col("Date_Key").alias("Ship_Date_Key")),
          F.col("f.Ship_Date") == F.col("Ship_Date_Value"), "inner") \
    .dropDuplicates()  # Ensure no duplicates after join

# Check row count again
print(fact_sales.count())  # This should match your expected number of rows

# Write to Delta
#fact_sales.write.format("delta").mode("overwrite").saveAsTable("FactSales")



# COMMAND ----------

# Alias the dim_date table twice
dim_date_order = dim_date.alias("do")
dim_date_ship = dim_date.alias("ds")

# Join FactSales with all dimension tables
fact_sales_with_data = fact_sales.alias("fs") \
    .join(dim_order.select("Order_Key", "Order_ID"), on="Order_Key", how="inner") \
    .join(dim_customer.select("Customer_Key", "Customer_ID", "Customer_Name", "Segment"), on="Customer_Key", how="inner") \
    .join(dim_product.select("Product_Key", "Product_ID", "Product_Name", "Category", "sub-category"), on="Product_Key", how="inner") \
    .join(dim_location.select("Location_Key", "Country", "State", "City"), on="Location_Key", how="inner") \
    .join(dim_date_order.select(F.col("Date_Key").alias("Order_Date_Key"), F.col("Order_Date").alias("Order_Date_Value")), 
          on="Order_Date_Key", how="inner") \
    .join(dim_date_ship.select(F.col("Date_Key").alias("Ship_Date_Key"), F.col("Order_Date").alias("Ship_Date_Value")), 
          on="Ship_Date_Key", how="inner")

# Select columns to view
fact_sales_with_data = fact_sales_with_data.select(
    "Fact_Key",
    "Order_ID",
    "Customer_ID",
    "Customer_Name",
    "Segment",
    "Product_ID",
    "Product_Name",
    "Category",
    "sub-category",
    "Country",
    "State",
    "City",
    "Order_Date_Value",
    "Ship_Date_Value",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost"
)

# Show result
fact_sales_with_data.show(10, truncate=False)


# COMMAND ----------

display(fact_sales_with_data)

# COMMAND ----------

fact_sales_with_data.count()

# COMMAND ----------

fact_data = fact_sales_with_data

# COMMAND ----------

display(fact_data)

# COMMAND ----------

fact_data.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/fact_data")\
        .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database gold_layer;

# COMMAND ----------

# Create a storage credential
spark.sql("""
CREATE STORAGE CREDENTIAL my_storage_credential
WITH (
  STORAGE_ACCOUNT_NAME = 'salesdatastorageaccount1',
  CONTAINER_NAME = 'superstore-sales-data',
  AZURE_MANAGED_IDENTITY = 'your-managed-identity'
)
""")

# Create an external location
spark.sql("""
CREATE EXTERNAL LOCATION my_external_location
WITH (
  URL = 'abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold',
  CREDENTIAL = my_storage_credential
)
""")