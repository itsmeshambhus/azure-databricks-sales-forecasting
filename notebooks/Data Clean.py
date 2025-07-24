# Databricks notebook source
spark

# COMMAND ----------


# COMMAND ----------

df = spark.read.format("csv").option("header","true").load(f"abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/bronze/Global_Superstore.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col, year, month, dayofmonth, quarter, monotonically_increasing_id


# COMMAND ----------

# Rename columns to snake_case by replacing spaces with underscores and converting to lowercase
new_columns = [col.replace(' ', '_').lower() for col in df.columns]

# Apply the new column names
df = df.toDF(*new_columns)

# Show the new column names
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Missing data

# COMMAND ----------

from pyspark.sql.functions import col, sum, when

# Compute missing values for each column
missing_counts = df.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns
])

# Convert the result to a dictionary for better formatting
missing_dict = missing_counts.collect()[0].asDict()

# Print each column with missing count on a new line
for col_name, missing_count in missing_dict.items():
    print(f"{col_name}: {missing_count}")

# COMMAND ----------

from pyspark.sql.functions import lit, when

clean_df = df.withColumn("postal_code", 
                when(col("postal_code").isNull(), lit("Unknown"))
                .otherwise(col("postal_code")))


# COMMAND ----------

display(clean_df)

# COMMAND ----------

# strings to dates
from pyspark.sql.functions import to_date

# Convert 'order_date' and 'ship_date' columns to date format
clean_df = clean_df.withColumn('order_date', to_date(clean_df['order_date'], 'dd-MM-yyyy')) \
             .withColumn('ship_date', to_date(clean_df['ship_date'], 'dd-MM-yyyy'))

# Show the updated DataFrame
clean_df.show()


# COMMAND ----------

# Add Year, Month, Day columns for Order Date
clean_df = clean_df.withColumn("order_year", year("order_date")) \
    .withColumn("order_month", month("order_date")) \
    .withColumn("order_day", dayofmonth("order_date")) \
    .withColumn("order_quarter", quarter("order_date"))

# COMMAND ----------

# Add Year, Month, Day columns for Ship Date
clean_df = clean_df.withColumn("ship_year", year("ship_date")) \
    .withColumn("ship_month", month("ship_Date")) \
    .withColumn("ship_day", dayofmonth("ship_Date")) \
    .withColumn("ship_Quarter", quarter("ship_Date"))

# COMMAND ----------

display(clean_df)

# COMMAND ----------

df.columns

# COMMAND ----------

# rows with duplicated data

from pyspark.sql.functions import count

# Group by all columns and count occurrences
duplicates_df = clean_df.groupBy(clean_df.columns).count().filter(col("count") > 1)

# Count total number of duplicate rows
total_duplicates = duplicates_df.selectExpr("sum(count - 1) as total_duplicates").collect()[0]["total_duplicates"]

# Print the total number of duplicate rows
print(f"Total duplicate rows: {total_duplicates}")

# COMMAND ----------

clean_df.write.mode("overwrite").parquet("abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/silver")


# COMMAND ----------

df = spark.read.format('parquet').load('abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/silver')

# COMMAND ----------

display(df)

# COMMAND ----------

unique_orderpriority = df.select("order_priority").distinct()
display(unique_orderpriority)

# COMMAND ----------

