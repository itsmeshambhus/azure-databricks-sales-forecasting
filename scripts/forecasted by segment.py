# Databricks notebook source

# COMMAND ----------

df = spark.read.format("csv").option("header","true").load(f"abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/bronze/Global_Superstore.csv")
display(df)

# COMMAND ----------

# Rename columns to snake_case by replacing spaces with underscores and converting to lowercase
new_columns = [col.replace(' ', '_').lower() for col in df.columns]

# Apply the new column names
df1 = df.toDF(*new_columns)

# Show the new column names
print(df1.columns)

# COMMAND ----------

# strings to dates
from pyspark.sql.functions import to_date

# Convert 'order_date' and 'ship_date' columns to date format
df1 = df1.withColumn('order_date', to_date(df1['order_date'], 'dd-MM-yyyy')) \
             .withColumn('ship_date', to_date(df1['ship_date'], 'dd-MM-yyyy'))

# Show the updated DataFrame
df1.show()

# COMMAND ----------

from pyspark.sql.functions import when, col

# Fill missing values in 'Postal Code'
df1 = df1.withColumn("Postal_Code", when(col("Postal_Code").isNull(), 0).otherwise(col("Postal_Code")))

# COMMAND ----------

from pyspark.sql.functions import date_format, sum as spark_sum

# Assuming there is a 'category' column in your dataframe
monthly_sales_by_segment  = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category"  # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "segment", "category", "sub-category")

# Show the result
display(monthly_sales_by_segment)

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_segment.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Create an empty DataFrame to store the forecast results
forecast_results = pd.DataFrame(columns=["Month", "Segment", "Category", "SubCategory", "ForecastedSales"])

# Get unique segments, categories, and sub-categories
segments = df_pandas['segment'].unique()

# Loop through each segment, category, and sub-category and apply Holt-Winters forecasting
for segment in segments:
    categories = df_pandas[df_pandas['segment'] == segment]['category'].unique()
    
    for category in categories:
        sub_categories = df_pandas[(df_pandas['segment'] == segment) & (df_pandas['category'] == category)]['sub-category'].unique()
        
        for sub_category in sub_categories:
            # Filter data for the specific segment, category, and sub-category
            sub_category_data = df_pandas[(df_pandas['segment'] == segment) & 
                                          (df_pandas['category'] == category) & 
                                          (df_pandas['sub-category'] == sub_category)]
            
            # Apply Holt-Winters Exponential Smoothing
            model = ExponentialSmoothing(sub_category_data['TotalSales'], 
                                          trend='add', 
                                          seasonal='add', 
                                          seasonal_periods=12)
            
            fitted_model = model.fit()
            forecast = fitted_model.forecast(12)
            
            # Create a date range for the forecast period
            last_date = sub_category_data.index[-1]
            forecast_index = pd.date_range(start=last_date, periods=12, freq='M')
            
            # Prepare the forecasted data and add it to the results DataFrame
            forecast_df = pd.DataFrame({
                "Month": forecast_index.strftime('%Y-%m'),  # Format to only include year and month (YYYY-MM)
                "Segment": segment,
                "Category": category,
                "SubCategory": sub_category,
                "ForecastedSales": forecast.values
            })
            
            # Append to the result DataFrame
            forecast_results = pd.concat([forecast_results, forecast_df], ignore_index=True)

# Reset the index for the final output
forecast_results.reset_index(drop=True, inplace=True)

# Show the forecasted sales by month, segment, category, and sub-category
display(forecast_results)

# COMMAND ----------

Segment = forecast_results

# COMMAND ----------

# Assuming 'Segment' is a Pandas DataFrame
spark_segment = spark.createDataFrame(Segment)

spark_segment.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/Forecaste/Segment/Segment")\
    .save()