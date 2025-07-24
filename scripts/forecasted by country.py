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
monthly_sales_by_country  = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category", "Country"  # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "segment", "category", "sub-category", "Country")

# Show the result
display(monthly_sales_by_country)

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_country.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

forecast_results = pd.DataFrame(columns=["Month", "Segment", "Category", "SubCategory", "Country", "ForecastedSales"])

categories = df_pandas['category'].unique()

for segment in df_pandas['segment'].unique():
    for country in df_pandas[df_pandas['segment'] == segment]['Country'].unique():
        for category in categories:
            sub_categories = df_pandas[
                (df_pandas['segment'] == segment) &
                (df_pandas['Country'] == country) &
                (df_pandas['category'] == category)
            ]['sub-category'].unique()
                
            for sub_category in sub_categories:
                sub_data = df_pandas[
                    (df_pandas['segment'] == segment) &
                    (df_pandas['Country'] == country) &
                    (df_pandas['category'] == category) &
                    (df_pandas['sub-category'] == sub_category)
                ][['TotalSales']].copy()

                # Pad missing months
                all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                sub_data = sub_data.reindex(all_months, fill_value=0)
                sub_data.index.name = 'Month'

                if len(sub_data) < 12:
                    print(f"📉 Skipped {country} | {segment} | {category} | {sub_category} — only {len(sub_data)} records")
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
                        "ForecastedSales": forecast.values
                    })

                    forecast_results = pd.concat([forecast_results, forecast_df])
                except Exception as e:
                    print(f"⚠️ Forecast error for {country} | {segment} | {category} | {sub_category}: {e}")

# Final cleanup
forecast_results.reset_index(drop=True, inplace=True)

# ✅ Prevent empty DataFrame display error
if not forecast_results.empty:
    display(forecast_results)
else:
    print("⚠️ No forecasts generated. All groups skipped due to insufficient data.")


# COMMAND ----------

from pyspark.sql.functions import date_format, sum as spark_sum

# Assuming there is a 'category' column in your dataframe
monthly_sales_by_country  = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category", "Region", "Country"  # Grouping by category as well
).agg(
    spark_sum("Sales").alias("TotalSales")
).orderBy("Month", "segment", "category", "sub-category", "Region", "Country")

# Show the result
display(monthly_sales_by_country)

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_country.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

forecast_results = pd.DataFrame(columns=["Month", "Segment", "Category", "SubCategory", "Region", "Country", "ForecastedSales"])

categories = df_pandas['category'].unique()

for segment in df_pandas['segment'].unique():
    for region in df_pandas[df_pandas['segment'] == segment]['Region'].unique():
        for country in df_pandas[(df_pandas['segment'] == segment) & (df_pandas['Region'] == region)]['Country'].unique():
            for category in categories:
                sub_categories = df_pandas[
                    (df_pandas['segment'] == segment) &
                    (df_pandas['Region'] == region) &
                    (df_pandas['Country'] == country) &
                    (df_pandas['category'] == category)
                ]['sub-category'].unique()
                
                for sub_category in sub_categories:
                    sub_data = df_pandas[
                        (df_pandas['segment'] == segment) &
                        (df_pandas['Region'] == region) &
                        (df_pandas['Country'] == country) &
                        (df_pandas['category'] == category) &
                        (df_pandas['sub-category'] == sub_category)
                    ][['TotalSales']].copy()

                    all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                    sub_data = sub_data.reindex(all_months, fill_value=0)
                    sub_data.index.name = 'Month'

                    if len(sub_data) < 12:
                        print(f"📉 Skipped {region} | {country} | {segment} | {category} | {sub_category} — only {len(sub_data)} records")
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
                            "Region": region,
                            "Country": country,
                            "ForecastedSales": forecast.values
                        })

                        forecast_results = pd.concat([forecast_results, forecast_df])
                    except Exception as e:
                        print(f"⚠️ Forecast error for {region} | {country} | {segment} | {category} | {sub_category}: {e}")

# Final cleanup
forecast_results.reset_index(drop=True, inplace=True)

# ✅ Show result
if not forecast_results.empty:
    display(forecast_results)
else:
    print("⚠️ No forecasts generated. All groups skipped due to insufficient data.")


# COMMAND ----------

# Assuming State is a Pandas DataFrame
Forecasted_Region = spark.createDataFrame(forecast_results)

Forecasted_Region.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/Forecaste/Region/Region")\
    .save()

# COMMAND ----------

from pyspark.sql.functions import date_format, sum as spark_sum

monthly_sales_by_country = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category", "Region", "Country"
).agg(
    spark_sum("Sales").alias("TotalSales"),
    spark_sum("Profit").alias("TotalProfit")  # ✅ Added
).orderBy("Month", "segment", "category", "sub-category", "Region", "Country")


# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_country.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

import pandas as pd
from statsmodels.tsa.holtwinters import ExponentialSmoothing

forecast_results = pd.DataFrame(columns=[
    "Month", "Segment", "Category", "SubCategory", "Region", "Country", "ForecastedSales", "ForecastedProfit"
])

categories = df_pandas['category'].unique()

for segment in df_pandas['segment'].unique():
    for region in df_pandas[df_pandas['segment'] == segment]['Region'].unique():
        for country in df_pandas[(df_pandas['segment'] == segment) & (df_pandas['Region'] == region)]['Country'].unique():
            for category in categories:
                sub_categories = df_pandas[
                    (df_pandas['segment'] == segment) &
                    (df_pandas['Region'] == region) &
                    (df_pandas['Country'] == country) &
                    (df_pandas['category'] == category)
                ]['sub-category'].unique()

                for sub_category in sub_categories:
                    sub_data = df_pandas[
                        (df_pandas['segment'] == segment) &
                        (df_pandas['Region'] == region) &
                        (df_pandas['Country'] == country) &
                        (df_pandas['category'] == category) &
                        (df_pandas['sub-category'] == sub_category)
                    ][['TotalSales', 'TotalProfit']].copy()

                    all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                    sub_data = sub_data.reindex(all_months, fill_value=0)
                    sub_data.index.name = 'Month'

                    if len(sub_data) < 12:
                        print(f"📉 Skipped {region} | {country} | {segment} | {category} | {sub_category} — only {len(sub_data)} records")
                        continue

                    try:
                        # Forecast Sales
                        sales_model = ExponentialSmoothing(
                            sub_data['TotalSales'], trend='add', seasonal='add', seasonal_periods=12
                        ).fit()
                        sales_forecast = sales_model.forecast(12)

                        # Forecast Profit
                        profit_model = ExponentialSmoothing(
                            sub_data['TotalProfit'], trend='add', seasonal='add', seasonal_periods=12
                        ).fit()
                        profit_forecast = profit_model.forecast(12)

                        forecast_df = pd.DataFrame({
                            "Month": sales_forecast.index.strftime('%Y-%m'),
                            "Segment": segment,
                            "Category": category,
                            "SubCategory": sub_category,
                            "Region": region,
                            "Country": country,
                            "ForecastedSales": sales_forecast.values,
                            "ForecastedProfit": profit_forecast.values
                        })

                        forecast_results = pd.concat([forecast_results, forecast_df])
                    except Exception as e:
                        print(f"⚠️ Forecast error for {region} | {country} | {segment} | {category} | {sub_category}: {e}")

# Final cleanup
forecast_results.reset_index(drop=True, inplace=True)

# ✅ Show result
if not forecast_results.empty:
    display(forecast_results)
else:
    print("⚠️ No forecasts generated. All groups skipped due to insufficient data.")


# COMMAND ----------

forecast_profit = forecast_results

# COMMAND ----------

# Assuming State is a Pandas DataFrame
forecast_profit = spark.createDataFrame(forecast_profit)

forecast_profit.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/Forecaste/Profit/Profit")\
    .save()

# COMMAND ----------

from pyspark.sql.functions import date_format, sum as spark_sum

monthly_sales_by_city = df1.groupBy(
    date_format("order_date", "yyyy-MM").alias("Month"),
    "segment", "category", "sub-category", "Region", "Country", "City", "Product_Name"
).agg(
    spark_sum("Sales").alias("TotalSales"),
    spark_sum("Profit").alias("TotalProfit"),
    spark_sum("Quantity").alias("TotalQuantity")
).orderBy("Month", "segment", "category", "sub-category", "Region", "Country", "City", "Product_Name")


# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
df_pandas = monthly_sales_by_city.toPandas()

# Ensure the 'Month' column is in datetime format
df_pandas['Month'] = pd.to_datetime(df_pandas['Month'], format='%Y-%m')

# Set the 'Month' column as the index
df_pandas.set_index('Month', inplace=True)

# COMMAND ----------

from statsmodels.tsa.holtwinters import ExponentialSmoothing

forecast_results = pd.DataFrame(columns=[
    "Month", "Segment", "Category", "SubCategory", "Region", "Country", "City", "Product_Name",
    "ForecastedSales", "ForecastedProfit", "ForecastedQuantity"
])

for segment in df_pandas['segment'].unique():
    seg_df = df_pandas[df_pandas['segment'] == segment]
    
    for category in seg_df['category'].unique():
        cat_df = seg_df[seg_df['category'] == category]
        
        for sub_category in cat_df['sub-category'].unique():
            sub_df = cat_df[cat_df['sub-category'] == sub_category]
            
            for region in sub_df['Region'].unique():
                region_df = sub_df[sub_df['Region'] == region]
                
                for country in region_df['Country'].unique():
                    country_df = region_df[region_df['Country'] == country]
                    
                    for city in country_df['City'].unique():
                        city_df = country_df[country_df['City'] == city]
                        
                        for product_name in city_df['Product_Name'].unique():
                            sub_data = city_df[city_df['Product_Name'] == product_name][[
                                'TotalSales', 'TotalProfit', 'TotalQuantity'
                            ]].copy()

                            # Fill missing months
                            all_months = pd.date_range(df_pandas.index.min(), df_pandas.index.max(), freq='MS')
                            sub_data = sub_data.reindex(all_months, fill_value=0)
                            sub_data.index.name = 'Month'

                            if len(sub_data) < 12:
                                print(f"📉 Skipped: {segment} | {category} | {sub_category} | {region} | {country} | {city} | {product_name} — only {len(sub_data)} records")
                                continue

                            try:
                                # Forecast Sales
                                sales_model = ExponentialSmoothing(
                                    sub_data['TotalSales'], trend='add', seasonal='add', seasonal_periods=12
                                ).fit()

                                # Forecast Profit
                                profit_model = ExponentialSmoothing(
                                    sub_data['TotalProfit'], trend='add', seasonal='add', seasonal_periods=12
                                ).fit()

                                # Forecast Quantity
                                quantity_model = ExponentialSmoothing(
                                    sub_data['TotalQuantity'], trend='add', seasonal='add', seasonal_periods=12
                                ).fit()

                                forecast_index = pd.date_range(sub_data.index[-1] + pd.offsets.MonthBegin(1), periods=12, freq='MS')

                                forecast_df = pd.DataFrame({
                                    "Month": forecast_index.strftime('%Y-%m'),
                                    "Segment": segment,
                                    "Category": category,
                                    "SubCategory": sub_category,
                                    "Region": region,
                                    "Country": country,
                                    "City": city,
                                    "Product_Name": product_name,
                                    "ForecastedSales": sales_model.forecast(12).values,
                                    "ForecastedProfit": profit_model.forecast(12).values,
                                    "ForecastedQuantity": quantity_model.forecast(12).values
                                })

                                forecast_results = pd.concat([forecast_results, forecast_df], ignore_index=True)

                            except Exception as e:
                                print(f"⚠️ Error for {segment} | {category} | {sub_category} | {region} | {country} | {city} | {product_name}: {e}")

# Display results
if not forecast_results.empty:
    display(forecast_results)
else:
    print("⚠️ No forecasts generated. All groups skipped due to insufficient data.")


# COMMAND ----------

forecast_city = forecast_results

# COMMAND ----------

# Assuming State is a Pandas DataFrame
forecast_city = spark.createDataFrame(forecast_city)

forecast_city.write.format("delta")\
    .mode("overwrite")\
    .option("path", "abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/gold/Forecaste/Cty/forecast_city")\
    .save()