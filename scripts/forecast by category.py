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

from pyspark.sql.functions import to_date, col, sum as spark_sum, date_format

# Convert Order Date to Date Type
df1 = df1.withColumn("OrderDate", to_date(col("order_date"), "MM/dd/yyyy"))

# Aggregate total sales by month
monthly_sales = df1.groupBy(date_format("OrderDate", "yyyy-MM").alias("Month")) \
                  .agg(spark_sum("Sales").alias("TotalSales")) \
                  .orderBy("Month")

monthly_sales.show()

# COMMAND ----------

# Assuming there is a 'category' column in your dataframe
monthly_sales_by_category = df1.groupBy(
    date_format("OrderDate", "yyyy-MM").alias("Month"),
    "category"  # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "category")

# Show the result
monthly_sales_by_category.show()


# COMMAND ----------

# Convert to Pandas
sales_pd = monthly_sales.toPandas()

# Rename columns for Prophet
sales_pd.rename(columns={'Month': 'ds', 'TotalSales': 'y'}, inplace=True)

# Convert 'ds' to datetime
import pandas as pd
sales_pd['ds'] = pd.to_datetime(sales_pd['ds'])

# COMMAND ----------

# Convert to Pandas
sales_pd = monthly_sales.toPandas()

# Convert 'Month' to datetime
import pandas as pd
sales_pd['Month'] = pd.to_datetime(sales_pd['Month'])

# Set Month as index
sales_pd.set_index('Month', inplace=True)

# Check final data
print(sales_pd.head())


# COMMAND ----------

import statsmodels.api as sm
import matplotlib.pyplot as plt

holt_winters_model = sm.tsa.ExponentialSmoothing(sales_pd['TotalSales'], trend='add', seasonal='add', seasonal_periods=12).fit()

forecast_hw = holt_winters_model.forecast(12)

# Plot actual and forecast
plt.figure(figsize=(12, 6))
plt.plot(sales_pd['TotalSales'], label='Actual Sales')
plt.plot(forecast_hw, label='Holt-Winters Forecast', color='blue')
plt.title('Global Superstore Sales Forecast (Holt-Winters Model)')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.legend()
plt.grid(True)
plt.show()


# COMMAND ----------

forecast_hw

# COMMAND ----------

# Convert to Pandas
sales_pd = monthly_sales_by_category.toPandas()

# Convert 'Month' to datetime
import pandas as pd
sales_pd['Month'] = pd.to_datetime(sales_pd['Month'])

# Ensure df1 has 'Category' column and convert to Pandas
if 'category' in df1.columns:
    sales_pd['category'] = df1.toPandas()['category']
else:
    raise KeyError("The 'category' column does not exist in df1")

# Set Month as index
sales_pd.set_index('Month', inplace=True)

# Check final data
display(sales_pd.head())

# COMMAND ----------

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_category.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import matplotlib.pyplot as plt

# Get unique categories
categories = df_pandas['category'].unique()

# Loop over each category
for category in categories:
    category_data = df_pandas[df_pandas['category'] == category]
    
    model = ExponentialSmoothing(category_data['TotalSales'], 
                                  trend='add', 
                                  seasonal='add', 
                                  seasonal_periods=12)
    
    fitted_model = model.fit()
    forecast = fitted_model.forecast(12)
    
    # Plotting (optional)
    plt.figure(figsize=(10, 6))
    plt.plot(category_data.index, category_data['TotalSales'], label='Historical Sales')
    plt.plot(forecast.index, forecast, label='Forecasted Sales', linestyle='--')
    plt.title(f"Sales Forecast for Category: {category}")
    plt.xlabel("Date")
    plt.ylabel("Total Sales")
    plt.legend()
    plt.show()


# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Create an empty DataFrame to store the results
forecast_results = pd.DataFrame(columns=["Month", "Category", "ForecastedSales"])

# Get unique categories
categories = df_pandas['category'].unique()

# Loop through each category and apply Holt-Winters forecasting
for category in categories:
    category_data = df_pandas[df_pandas['category'] == category]
    
    # Apply Holt-Winters Exponential Smoothing
    model = ExponentialSmoothing(category_data['TotalSales'], 
                                  trend='add', 
                                  seasonal='add', 
                                  seasonal_periods=12)
    
    fitted_model = model.fit()
    forecast = fitted_model.forecast(12)
    
    # Prepare the forecasted data and add it to the results DataFrame
    forecast_df = pd.DataFrame({
        "Month": forecast.index,
        "Category": category,
        "ForecastedSales": forecast.values
    })
    
    # Append to the result DataFrame
    forecast_results = pd.concat([forecast_results, forecast_df])

# Reset the index for the final output
forecast_results.reset_index(drop=True, inplace=True)

# Show the forecasted sales by month and category
forecast_results
