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
monthly_sales_by_state  = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category", "Country", "State"  # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "segment", "category", "sub-category", "Country", "State")

# Show the result
display(monthly_sales_by_state)

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_state.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Initialize the final DataFrame
forecast_results = pd.DataFrame(columns=["Month", "Segment", "Category", "SubCategory", "Country", "State", "ForecastedSales"])

# Loop through each combination
for segment in df_pandas['segment'].unique():
    for country in df_pandas[df_pandas['segment'] == segment]['Country'].unique():
        for state in df_pandas[(df_pandas['segment'] == segment) & (df_pandas['Country'] == country)]['State'].unique():
            for category in df_pandas['category'].unique():
                sub_categories = df_pandas[
                    (df_pandas['segment'] == segment) &
                    (df_pandas['Country'] == country) &
                    (df_pandas['State'] == state) &
                    (df_pandas['category'] == category)
                ]['sub-category'].unique()

                for sub_category in sub_categories:
                    sub_data = df_pandas[
                        (df_pandas['segment'] == segment) &
                        (df_pandas['Country'] == country) &
                        (df_pandas['State'] == state) &
                        (df_pandas['category'] == category) &
                        (df_pandas['sub-category'] == sub_category)
                    ][['TotalSales']].copy()

                    # Pad missing months
                    all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                    sub_data = sub_data.reindex(all_months, fill_value=0)
                    sub_data.index.name = 'Month'

                    if len(sub_data) < 24:
                        print(f"📉 Skipped {country} | {state} | {segment} | {category} | {sub_category} — only {len(sub_data)} records")
                        continue

                    try:
                        model = ExponentialSmoothing(
                            sub_data['TotalSales'], trend='add', seasonal='add', seasonal_periods=12
                        )
                        fitted_model = model.fit()
                        forecast = fitted_model.forecast(12)

                        forecast_df = pd.DataFrame({
                            "Month": forecast.index.strftime('%Y-%m'),
                            "Segment": segment,
                            "Category": category,
                            "SubCategory": sub_category,
                            "Country": country,
                            "State": state,
                            "ForecastedSales": forecast.values
                        })

                        forecast_results = pd.concat([forecast_results, forecast_df])

                    except Exception as e:
                        print(f"⚠️ Forecast error for {country} | {state} | {segment} | {category} | {sub_category}: {e}")

# Reset index
forecast_results.reset_index(drop=True, inplace=True)

# Display final result
if not forecast_results.empty:
    display(forecast_results)
else:
    print("⚠️ No forecasts generated due to insufficient data.")


# COMMAND ----------

State = forecast_results

# COMMAND ----------

# Assuming State is a Pandas DataFrame
spark_df = spark.createDataFrame(State)

spark_df.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/Forecaste/State/State")\
    .save()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import date_format, sum as spark_sum

# Assuming there is a 'category' column in your dataframe
monthly_sales_by_region  = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category","Region", "Country", "State"   # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "segment", "category", "sub-category", "Region", "Country", "State" )

# Show the result
display(monthly_sales_by_region)

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_region.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Initialize the final DataFrame
forecast_region = pd.DataFrame(columns=[
    "Month", "Segment", "Category", "SubCategory", "Region", "Country", "State", "ForecastedSales"
])

# Loop through each combination
for region in df_pandas['Region'].unique():
    for segment in df_pandas[df_pandas['Region'] == region]['segment'].unique():
        for country in df_pandas[(df_pandas['Region'] == region) & (df_pandas['segment'] == segment)]['Country'].unique():
            for state in df_pandas[
                (df_pandas['Region'] == region) & 
                (df_pandas['segment'] == segment) & 
                (df_pandas['Country'] == country)
            ]['State'].unique():
                for category in df_pandas['category'].unique():
                    sub_categories = df_pandas[
                        (df_pandas['Region'] == region) &
                        (df_pandas['segment'] == segment) &
                        (df_pandas['Country'] == country) &
                        (df_pandas['State'] == state) &
                        (df_pandas['category'] == category)
                    ]['sub-category'].unique()

                    for sub_category in sub_categories:
                        sub_data = df_pandas[
                            (df_pandas['Region'] == region) &
                            (df_pandas['segment'] == segment) &
                            (df_pandas['Country'] == country) &
                            (df_pandas['State'] == state) &
                            (df_pandas['category'] == category) &
                            (df_pandas['sub-category'] == sub_category)
                        ][['TotalSales']].copy()

                        # Pad missing months
                        all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                        sub_data = sub_data.reindex(all_months, fill_value=0)
                        sub_data.index.name = 'Month'

                        if len(sub_data) < 24:
                            print(f"📉 Skipped {region} | {country} | {state} | {segment} | {category} | {sub_category} — only {len(sub_data)} records")
                            continue

                        try:
                            model = ExponentialSmoothing(
                                sub_data['TotalSales'], trend='add', seasonal='add', seasonal_periods=12
                            )
                            fitted_model = model.fit()
                            forecast = fitted_model.forecast(12)

                            forecast_df = pd.DataFrame({
                                "Month": forecast.index.strftime('%Y-%m'),
                                "Region": region,
                                "Segment": segment,
                                "Category": category,
                                "SubCategory": sub_category,
                                "Country": country,
                                "State": state,
                                "ForecastedSales": forecast.values
                            })

                            forecast_region = pd.concat([forecast_results, forecast_df])

                        except Exception as e:
                            print(f"⚠️ Forecast error for {region} | {country} | {state} | {segment} | {category} | {sub_category}: {e}")

# Reset index
forecast_region.reset_index(drop=True, inplace=True)

# Display final result
if not forecast_region.empty:
    display(forecast_region)
else:
    print("⚠️ No forecasts generated due to insufficient data.")
