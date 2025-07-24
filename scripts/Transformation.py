# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

df = spark.read.format('parquet').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/silver')
df.display()

# COMMAND ----------

df.count()

# COMMAND ----------

df

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define a window spec (no partitioning since we want continuous numbers)
windowSpec = Window.orderBy("Product_ID")

dim_product = df.select(
    "Product_ID", "Product_Name", "Category", "Sub-Category"
).dropDuplicates()

# Add sequential surrogate key starting from 1
dim_product = dim_product.withColumn("Product_Key", row_number().over(windowSpec))

# Reorder columns: surrogate key first
dim_product = dim_product.select("Product_Key", "Product_ID", "Product_Name", "Category", "Sub-Category")

# Save as Delta table with overwriteSchema option
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("DimProduct")



# COMMAND ----------

dim_product.count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Define window spec to pick the first occurrence per Product_ID
windowSpec = Window.partitionBy("Product_ID").orderBy("Product_Name")  # or any other tie-breaker

# Add row_number within each Product_ID group
dim_product = df.select(
    "Product_ID", "Product_Name", "Category", "Sub-Category"
).withColumn("row_num", row_number().over(windowSpec))

# Keep only the first occurrence of each Product_ID
dim_product = dim_product.filter("row_num = 1").drop("row_num")

# Add surrogate key
dim_product = dim_product.withColumn("Product_Key", row_number().over(Window.orderBy("Product_ID")))

# Reorder columns
dim_product = dim_product.select("Product_Key", "Product_ID", "Product_Name", "Category", "Sub-Category")

# Save to Delta
dim_product.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("DimProduct")


# COMMAND ----------

dim_product.count()

# COMMAND ----------

dim_product.display()

# COMMAND ----------

# Create the Customer Dimension table without location details
dim_customer = df.select(
    "Customer_ID", "Customer_Name", "Segment"
).dropDuplicates()

# Create a window specification for sequential surrogate key
windowSpec = Window.orderBy("Customer_ID")

# Add surrogate key starting from 1
dim_customer = dim_customer.withColumn("Customer_Key", row_number().over(windowSpec))

# Reorder columns to place surrogate key first
dim_customer = dim_customer.select("Customer_Key", "Customer_ID", "Customer_Name", "Segment")

# Write the dimension to Delta format
dim_customer.write.format("delta").mode("overwrite").saveAsTable("DimCustomer")

display(dim_customer)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Create the Location Dimension table
dim_location = df.select(
    "Customer_ID", "Country", "Region", "State", "City", "Market", "Postal_Code"
).dropDuplicates()

# Define window specification
windowSpec = Window.orderBy("Customer_ID")

# Add surrogate key starting from 1
dim_location = dim_location.withColumn("Location_Key", row_number().over(windowSpec))

# Reorder columns to place surrogate key first
dim_location = dim_location.select(
    "Location_Key", "Customer_ID", "Country", "Region", "State", "City", "Market", "Postal_Code"
)

# Write the dimension to Delta format
dim_location.write.format("delta").mode("overwrite").saveAsTable("DimLocation")

display(dim_location)

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, quarter, date_format, row_number
from pyspark.sql.window import Window

# Drop duplicate dates to keep one record per unique date
dim_date = df.select("Order_Date").dropDuplicates()

# Add derived date attributes
dim_date = dim_date.withColumn("Order_Year", year(col("Order_Date"))) \
                   .withColumn("Order_Month", month(col("Order_Date"))) \
                   .withColumn("Order_Month_Name", date_format(col("Order_Date"), "MMMM")) \
                   .withColumn("Order_Quarter", quarter(col("Order_Date"))) \
                   .withColumn("Order_Day", dayofmonth(col("Order_Date"))) \
                   .withColumn("Order_WeekDay", date_format(col("Order_Date"), "EEEE"))

# Add surrogate Date_Key
windowSpec = Window.orderBy("Order_Date")
dim_date = dim_date.withColumn("Date_Key", row_number().over(windowSpec))

# Reorder columns
dim_date = dim_date.select("Date_Key", "Order_Date", "Order_Year", "Order_Month", 
                           "Order_Month_Name", "Order_Quarter", "Order_Day", "Order_WeekDay")

# Write the DimDate table
dim_date.write.format("delta").mode("overwrite").saveAsTable("DimDate")

display(dim_date)


# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

# Define order priority rank
priority_rank = F.when(F.col("Order_Priority") == "Critical", 1) \
                 .when(F.col("Order_Priority") == "High", 2) \
                 .when(F.col("Order_Priority") == "Medium", 3) \
                 .when(F.col("Order_Priority") == "Low", 4) \
                 .otherwise(5)

# Add priority rank column
df = df.withColumn("Priority_Rank", priority_rank)

# Window by Order_ID to get earliest Ship_Date and highest priority
order_window = Window.partitionBy("Order_ID")

dim_order = df.groupBy("Order_ID").agg(
    F.min("Ship_Date").alias("Ship_Date"),
    F.min("Priority_Rank").alias("Priority_Rank"),
    F.first("Ship_Mode").alias("Ship_Mode")  # Assuming first ship mode is fine; or mode() if needed
)

# Convert Priority_Rank back to label
dim_order = dim_order.withColumn("Order_Priority",
    F.when(F.col("Priority_Rank") == 1, "Critical")
     .when(F.col("Priority_Rank") == 2, "High")
     .when(F.col("Priority_Rank") == 3, "Medium")
     .when(F.col("Priority_Rank") == 4, "Low")
     .otherwise("Unknown")
).drop("Priority_Rank")

# Add surrogate key
windowSpec = Window.orderBy("Order_ID")
dim_order = dim_order.withColumn("Order_Key", F.row_number().over(windowSpec))

# Reorder columns
dim_order = dim_order.select("Order_Key", "Order_ID", "Ship_Date", "Ship_Mode", "Order_Priority")

# Write to Delta table
dim_order.write.format("delta").mode("overwrite").saveAsTable("DimOrder")

display(dim_order)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Drop duplicate orders to keep one record per unique order
dim_order = df.select("Order_ID", "Ship_Date", "Ship_Mode", "Order_Priority").dropDuplicates()

# Add surrogate key (Order_Key)
windowSpec = Window.orderBy("Order_ID")
dim_order = dim_order.withColumn("Order_Key", row_number().over(windowSpec))

# Reorder columns to have surrogate key first
dim_order = dim_order.select("Order_Key", "Order_ID", "Ship_Date", "Ship_Mode", "Order_Priority")

# Write the DimOrder table
dim_order.write.format("delta").mode("overwrite").saveAsTable("DimOrder")

display(dim_order)

# COMMAND ----------

# Load Dimension Tables
dim_order = spark.table("DimOrder")
dim_customer = spark.table("DimCustomer")
dim_product = spark.table("DimProduct")
dim_location = spark.table("DimLocation")
dim_date = spark.table("DimDate")

# COMMAND ----------

display(dim_location)

# COMMAND ----------

from pyspark.sql import functions as F


# COMMAND ----------

dim_date_order = dim_date.alias("do")
dim_date_ship = dim_date.alias("ds")

fact_sales = df.alias("f") \
    .join(dim_order.select("Order_ID", "Order_Key"), on="Order_ID", how="inner") \
    .join(dim_customer.select("Customer_ID", "Customer_Key"), on="Customer_ID", how="inner") \
    .join(dim_product.select("Product_ID", "Product_Key"), on="Product_ID", how="inner") \
    .join(dim_location.select("Country", "State", "City", "Location_Key"),
          on=["Country", "State", "City"], how="inner") \
    .join(dim_date_order.select(F.col("Order_Date").alias("Order_Date_Value"), F.col("Date_Key").alias("Order_Date_Key")),
          F.col("f.Order_Date") == F.col("Order_Date_Value"), "inner") \
    .join(dim_date_ship.select(F.col("Order_Date").alias("Ship_Date_Value"), F.col("Date_Key").alias("Ship_Date_Key")),
          F.col("f.Ship_Date") == F.col("Ship_Date_Value"), "inner")


# COMMAND ----------

from pyspark.sql import functions as F

dim_date_order = dim_date.alias("do")
dim_date_ship = dim_date.alias("ds")

fact_sales = df.alias("f") \
    .join(dim_order.select("Order_ID", "Order_Key"), on="Order_ID", how="inner") \
    .join(dim_customer.select("Customer_ID", "Customer_Key"), on="Customer_ID", how="inner") \
    .join(dim_product.select("Product_ID", "Product_Key"), on="Product_ID", how="inner") \
    .join(
    dim_location.select(
        "Country", "State", "City", "Location_Key",
        #F.col("Region").alias("Location_Region"),
        #F.col("Market").alias("Location_Market")
    ),
    on=["Country", "State", "City"],
    how="inner"
    )\
    .join(
    dim_date_order.select(
        F.col("Order_Date").alias("Order_Date_Value"),
        F.col("Date_Key").alias("Order_Date_Key"),
        F.col("Order_Year").alias("Order_Year_Value"),
        #F.col("Order_Month").alias("Order_Month_Value")
    ),
    F.col("f.Order_Date") == F.col("Order_Date_Value"),
    "inner"
    )\
    .join(
        dim_date_ship.select(
            F.col("Order_Date").alias("Ship_Date_Value"),
            F.col("Date_Key").alias("Ship_Date_Key")
        ),
        F.col("f.Ship_Date") == F.col("Ship_Date_Value"),
        "inner"
    )


# COMMAND ----------

fact_sales = fact_sales.drop("Ship_Date_Key", "Ship_Date_Value", "Order_Date_Key", "Order_Date_Value", "Location_Key", "Product_Key", "Customer_Key", "Order_Key")

# COMMAND ----------


display(fact_sales)

# COMMAND ----------

display(fact_sales)

# COMMAND ----------

fact_sales.count()

# COMMAND ----------



# Define window spec for Fact_Key
fact_window = Window.orderBy(F.lit(1))  # Dummy ordering for sequential numbering

# Add Fact_Key column
fact_sales = fact_sales.withColumn("Fact_Key", F.row_number().over(fact_window))

# Reorder columns: Fact_Key first, followed by keys and new columns
fact_sales = fact_sales.select(
    "Fact_Key",
    "Order_Key",
    "Customer_Key",
    "Product_Key",
    "Location_Key",
    "Order_Date_Key",
    "Ship_Date_Key",
    "Order_Month",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost"
)

# Write updated Fact Table to Delta (if required)
# fact_sales.write.format("delta").mode("overwrite").saveAsTable("FactSales")

print("✅ FactSales table with Fact_Key, Region, Market, Year, and Month created successfully.")


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Define window spec for Fact_Key
fact_window = Window.orderBy(F.lit(1))  # Dummy ordering for sequential numbering

# Add Fact_Key column
fact_sales = fact_sales.withColumn("Fact_Key", F.row_number().over(fact_window))

# Reorder columns: Fact_Key first, followed by keys and new columns
fact_sales = fact_sales.select(
    "Fact_Key",
    "Order_Key",
    "Customer_Key",
    "Product_Key",
    "Location_Key",
    "Order_Date_Key",
    "Ship_Date_Key",
    "Region",
    "Market",
    "Order_Year",
    "Order_Month",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost"
)

# Write updated Fact Table to Delta (if required)
# fact_sales.write.format("delta").mode("overwrite").saveAsTable("FactSales")

print("✅ FactSales table with Fact_Key, Region, Market, Year, and Month created successfully.")


# COMMAND ----------

from pyspark.sql.window import Window

# Define window spec for Fact_Key
fact_window = Window.orderBy(F.lit(1))  # Just a dummy ordering for sequential numbering

# Add Fact_Key column
fact_sales = fact_sales.withColumn("Fact_Key", F.row_number().over(fact_window))

# Reorder columns: Fact_Key first
fact_sales = fact_sales.select(
    "Fact_Key",
    "Order_Key",
    "Customer_Key",
    "Product_Key",
    "Location_Key",
    "Order_Date_Key",
    "Ship_Date_Key",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost"
)

# Write updated Fact Table to Delta
#fact_sales.write.format("delta").mode("overwrite").saveAsTable("FactSales")

print("✅ FactSales table with Fact_Key created successfully.")


# COMMAND ----------

display(fact_sales)

# COMMAND ----------

fact_sales_filtered = fact_sales.filter((fact_sales["Order_Key"] >= 1) & (fact_sales["Order_Key"] <= 15))
display(fact_sales_filtered)

# COMMAND ----------

fact_sales.count()

# COMMAND ----------

dim_order.select("Order_ID").distinct().count()
dim_customer.select("Customer_ID").distinct().count()
dim_product.select("Product_ID").distinct().count()
dim_location.select("Country", "State", "City").distinct().count()
dim_date.select("Order_Date").distinct().count()


# COMMAND ----------

print(fact_sales.count())
print(fact_sales.dropDuplicates().count())


# COMMAND ----------

df.count()

# COMMAND ----------

dim_date.groupBy("Order_Date").count().orderBy(F.desc("count")).show()


# COMMAND ----------

# Check frequency of each Customer_ID in the original data
dim_customer.groupBy("Customer_ID").count().orderBy(F.desc("count")).show()


# COMMAND ----------

# Check for duplicates in dim_product
dim_product_duplicates = dim_product.groupBy("Product_ID").count().filter("count > 1")
dim_product_duplicates.show()


# COMMAND ----------

dim_location.groupBy("Country", "State", "City", "Region", "market").count().orderBy(F.desc("count")).show()


# COMMAND ----------

dim_order.groupBy("Order_ID").count().orderBy(F.desc("count")).show()


# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Deduplicate dim_order based on Order_ID
dim_order = df.select("Order_ID").dropDuplicates()

# Then, add surrogate key (Order_Key)
windowSpec = Window.orderBy("Order_ID")

dim_order = dim_order.withColumn("Order_Key", row_number().over(windowSpec))

# Reorder columns to put Order_Key first
dim_order = dim_order.select("Order_Key", "Order_ID")

display(dim_order)

# COMMAND ----------

dim_order.count()

# COMMAND ----------

dim_location = df.select("Country", "State", "Region", "City", "market").dropDuplicates()

windowSpec = Window.orderBy("Country", "State", "Region", "Market", "City")

dim_location = dim_location.withColumn("Location_Key", row_number().over(windowSpec))

dim_location = dim_location.select("Location_Key", "Country", "State", "Region", "Market", "City")


# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# Select distinct products and add a surrogate key to the product dimension
dim_product = df.select("Product_ID", "Product_Name", "Category", "sub-category").dropDuplicates()

# Define the window specification
windowSpec = Window.orderBy("Product_ID")

# Add a surrogate key starting from 1
dim_product = dim_product.withColumn("Product_Key", row_number().over(windowSpec))

# Reorder columns
dim_product = dim_product.select("Product_Key", "Product_ID", "Product_Name", "Category", "sub-category")

# Write to Delta format
dim_product.write.format("delta").mode("overwrite").saveAsTable("DimProduct")

# COMMAND ----------

dim_customer.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_customer")\
        .save()

# COMMAND ----------

dim_product.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_product")\
        .save()

# COMMAND ----------

dim_order.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_order")\
        .save()

# COMMAND ----------

dim_location.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_location")\
        .save()

# COMMAND ----------

dim_date.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_date")\
        .save()

# COMMAND ----------

dim_customer.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/dim_customer")\
        .save()

# COMMAND ----------

fact_sales.write.format("delta")\
    .mode("overwrite")\
        .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/fact_sales_final")\
        .save()

# COMMAND ----------

fact_sales.count()

# COMMAND ----------

dim_customer

# COMMAND ----------

display(dim_product)

# COMMAND ----------

from pyspark.sql import functions as F

dim_date_order = dim_date.alias("do")
dim_date_ship = dim_date.alias("ds")

fact_sales = df.alias("f") \
    .join(dim_order.select("Order_ID", "Order_Key"), on="Order_ID", how="inner") \
    .join(dim_customer.select("Customer_ID", "Customer_Key"), on="Customer_ID", how="inner") \
    .join(dim_product.select("Product_ID", "Product_Key"), on="Product_ID", how="inner") \
    .join(
    dim_location.select(
        "Country", "State", "City", "Location_Key",
        F.col("Region").alias("Location_Region"),
        F.col("Market").alias("Location_Market")
    ),
    on=["Country", "State", "City"],
    how="inner"
    )\
    .join(
    dim_date_order.select(
        F.col("Order_Date").alias("Order_Date_Value"),
        F.col("Date_Key").alias("Order_Date_Key"),
        F.col("Order_Year").alias("Order_Year_Value"),
        F.col("Order_Month").alias("Order_Month_Value")
    ),
    F.col("f.Order_Date") == F.col("Order_Date_Value"),
    "inner"
    )\
    .join(
        dim_date_ship.select(
            F.col("Order_Date").alias("Ship_Date_Value"),
            F.col("Date_Key").alias("Ship_Date_Key")
        ),
        F.col("f.Ship_Date") == F.col("Ship_Date_Value"),
        "inner"
    )


# Select relevant columns to view
fact_sales_with_data = fact_sales_with_data.select(
    "fs.Fact_Key",
    "fs.Order_ID",
    "fs.Sales",
    "fs.Quantity",
    "fs.Discount",
    "fs.Profit",
    "fs.Shipping_Cost",
    "Customer_Name",
    "Segment",
    "Product_Name",
    "Category",
    "sub-category",
    "Country",
    "State",
    "City",
    "Order_Date",
    "Ship_Date"
)

# Display the data (for example, displaying first 10 records)
display(fact_sales_with_data.limit(10))


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Aliasing dim_date for order and shipment
dim_date_order = dim_date.alias("do")
dim_date_ship = dim_date.alias("ds")

# Building the fact_sales DataFrame with all necessary joins
fact_sales = df.alias("f") \
    .join(dim_order.select("Order_ID", "Order_Key"), on="Order_ID", how="inner") \
    .join(dim_customer.select("Customer_ID", "Customer_Key"), on="Customer_ID", how="inner") \
    .join(dim_product.select("Product_ID", "Product_Key"), on="Product_ID", how="inner") \
    .join(
        dim_location.select(
            "Country", "State", "City", "Location_Key", "Region", "Market"
        ),
        on=["Country", "State", "City"],
        how="inner"
    ) \
    .join(
        dim_date_order.select(
            F.col("Order_Date").alias("Order_Date_Value"),
            F.col("Date_Key").alias("Order_Date_Key"),
            F.col("Order_Year").alias("Order_Year"),
            F.col("Order_Month").alias("Order_Month")
        ),
        F.col("f.Order_Date") == F.col("Order_Date_Value"),
        "inner"
    ) \
    .join(
        dim_date_ship.select(
            F.col("Order_Date").alias("Ship_Date_Value"),
            F.col("Date_Key").alias("Ship_Date_Key")
        ),
        F.col("f.Ship_Date") == F.col("Ship_Date_Value"),
        "inner"
    )

# Define window spec for Fact_Key
fact_window = Window.orderBy(F.lit(1))  # Dummy ordering for sequential numbering

# Add Fact_Key column
fact_sales = fact_sales.withColumn("Fact_Key", F.row_number().over(fact_window))

# Reorder columns: Fact_Key first, followed by keys and new columns
fact_sales = fact_sales.select(
    "Fact_Key",
    "Order_Key",
    "Customer_Key",
    "Product_Key",
    "Location_Key",
    "Order_Date_Key",
    "Ship_Date_Key",
    "Region",
    "Market",
    "Order_Year",
    "Order_Month",
    "Sales",
    "Quantity",
    "Discount",
    "Profit",
    "Shipping_Cost"
)

# Write updated Fact Table to Delta (if required)
# fact_sales.write.format("delta").mode("overwrite").saveAsTable("FactSales")

print("✅ FactSales table with Fact_Key, Region, Market, Year, and Month created successfully.")


# COMMAND ----------

display(fact_sales_display)

# COMMAND ----------

fact_data = spark.read.format('delta').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/fact_data')
fact_data.display()