# Databricks notebook source
spark

# COMMAND ----------


# COMMAND ----------

df = spark.read.format("csv").option("header","true").load(f"abfss://superstore-sales-data@salesdatastorageaccount1.dfs.core.windows.net/bronze/Global_Superstore.csv")
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

#
df.printSchema()

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.count()

# COMMAND ----------

# Get number of rows
num_rows = df.count()

# Get number of columns
num_cols = len(df.columns)

# Print the shape
print(f"DataFrame shape: ({num_rows}, {num_cols})")


# COMMAND ----------



# COMMAND ----------

# data summary statistics

# Compute summary statistics
summary_df = df.describe()

# Convert to dictionary for better readability
summary_data = summary_df.collect()

# Print each statistic on a new line
for row in summary_data:
    print(f"{row['summary']}:")
    for col_name in df.columns:
        print(f"  {col_name}: {row[col_name]}")
    display("-" * 50)  # Separator for better readability


# COMMAND ----------

# columns with missing data
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

# rows with duplicated data

from pyspark.sql.functions import count

# Group by all columns and count occurrences
duplicates_df = df.groupBy(df.columns).count().filter(col("count") > 1)

# Count total number of duplicate rows
total_duplicates = duplicates_df.selectExpr("sum(count - 1) as total_duplicates").collect()[0]["total_duplicates"]

# Print the total number of duplicate rows
print(f"Total duplicate rows: {total_duplicates}")


# COMMAND ----------

# make a copy of the data before cleaning
df1 = df.select("*")  # Creates a new DataFrame with the same data


# COMMAND ----------

# Rename columns to snake_case by replacing spaces with underscores and converting to lowercase
new_columns = [col.replace(' ', '_').lower() for col in df1.columns]

# Apply the new column names
df1 = df1.toDF(*new_columns)

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

# confirm changes
# Show the schema to confirm the data types of the columns
df1.select('order_date', 'ship_date').printSchema()


# COMMAND ----------

from pyspark.sql.functions import to_date, min, max

# Convert 'order_date' to date format (dd-MM-yyyy)
df1 = df1.withColumn('order_date', to_date(df1['order_date'], 'dd-MM-yyyy'))

# Calculate the first and last order date
first_order_date = df1.agg(min('order_date')).collect()[0][0]
last_order_date = df1.agg(max('order_date')).collect()[0][0]

# Print the results
print(f"The first order date is: {first_order_date}")
print(f"The last order date is: {last_order_date}")


# COMMAND ----------

df1.show()

# COMMAND ----------

# create a new column sales_year
from pyspark.sql.functions import year

# Create a new column 'sales_year' by extracting the year from 'order_date'
df1 = df1.withColumn('sales_year', year(df1['order_date']))

# Show the updated DataFrame with the 'sales_year' column
df1.select('order_date', 'sales_year').show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col

# List of columns to convert to different types
cols_string = ['ship_mode', 'segment', 'state', 'country', 'region', 'market', 'category', 'sub-category', 'order_priority']
cols_object = ['order_id', 'customer_id', 'customer_name', 'city', 'product_id', 'product_name']
cols_float64 = ['postal_code', 'sales', 'discount', 'profit', 'shipping_cost']
cols_int64 = ['row_id', 'quantity', 'sales_year']

# Convert columns to StringType (for categorical columns)
for c in cols_string:
    df1 = df1.withColumn(c, col(c).cast('string'))

# Convert columns to ObjectType (ObjectType in PySpark is equivalent to StringType in many cases)
for c in cols_object:
    df1 = df1.withColumn(c, col(c).cast('string'))  # Equivalent to object in Pandas

# Convert columns to FloatType (for numerical columns)
for c in cols_float64:
    df1 = df1.withColumn(c, col(c).cast('float'))

# Convert columns to IntegerType (for integer columns)
for c in cols_int64:
    df1 = df1.withColumn(c, col(c).cast('int'))

# Show the updated DataFrame schema
df1.printSchema()


# COMMAND ----------

# Fill missing values in 'Postal Code'
df1 = df1.withColumn("Postal_Code", when(col("Postal_Code").isNull(), 0).otherwise(col("Postal_Code")))


# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis and Data Visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Total sales per category

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'category' and sum the 'sales' column
sales_category = df1.groupBy('category').agg(F.sum('sales').alias('total_sales'))

# Sort the result by 'total_sales' in descending order
sales_category = sales_category.orderBy(F.col('total_sales').desc())

# Show the result
sales_category.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Total profits per Category

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'category' and sum the 'profit' column
profit_category = df1.groupBy('category').agg(F.sum('profit').alias('total_profit'))

# Sort the result by 'total_profit' in descending order
profit_category = profit_category.orderBy(F.col('total_profit').desc())

# Show the result
profit_category.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Plot pie charts showing percentage of categories in sales and profit totals.

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

# Group total sales by category
sales_category = df1.groupBy('category').agg(F.sum('sales').alias('total_sales')).toPandas()

# Group total profits by category
profit_category = df1.groupBy('category').agg(F.sum('profit').alias('total_profits')).toPandas()

# Figure size
plt.figure(figsize=(16,12))

# Left: Total sales pie chart
plt.subplot(1,2,1)  # 1 row, 2 columns, 1st plot
plt.pie(sales_category['total_sales'], labels=sales_category['category'], startangle=90, counterclock=False,
        autopct=lambda p: f'{p:.1f}% \n ${p * np.sum(sales_category["total_sales"]) / 100:,.0f}',
        wedgeprops={'linewidth': 1, 'edgecolor': 'black', 'alpha': 0.75})
plt.axis('square')
plt.title('Total Sales by Category', fontdict={'fontsize':16})

# Right: Total profits pie chart
plt.subplot(1,2,2)  # 1 row, 2 columns, 2nd plot
plt.pie(profit_category['total_profits'], labels=profit_category['category'], startangle=90, counterclock=False,
        autopct=lambda p: f'{p:.1f}% \n ${p * np.sum(profit_category["total_profits"]) / 100:,.0f}',
        wedgeprops={'linewidth': 1, 'edgecolor': 'black', 'alpha': 0.75})
plt.axis('square')
plt.title('Total Profit by Category', fontdict={'fontsize':16})

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Total sales
# MAGIC
# MAGIC - Technology at 37.5%
# MAGIC - Furniture at 32.5%
# MAGIC - Office supplies at 30%
# MAGIC
# MAGIC The sales are almost divided equally among the three categories. Each category account for over 30% of total sales
# MAGIC
# MAGIC Total profits
# MAGIC
# MAGIC - Technology at 45.2%
# MAGIC - Office supplies at 35.3%
# MAGIC - Furniture at 19.4%
# MAGIC
# MAGIC Interestingly, Furniture has a lower percentage of profits raked in compared to its share of percentage sale.

# COMMAND ----------

# MAGIC %md
# MAGIC Let us look at the subcategories to understand which subcategories contribute to lower profits for furniture and which sub categories contribute to higher profits for the Technology category.

# COMMAND ----------

# Grouping the data by 'category' and 'sub-category' and calculating the sum of 'sales' and 'profit'
sales_per_subcategory = df1.groupBy(['category', 'sub-category']).agg(
    sum('sales').alias('total_sales'),
    sum('profit').alias('total_profit')
)

# Create a new column for 'profit_margin' as profit divided by sales
sales_per_subcategory = sales_per_subcategory.withColumn(
    'profit_margin',
    sales_per_subcategory['total_profit'] / sales_per_subcategory['total_sales']
)

# Sorting the dataframe by 'profit_margin' in descending order
sales_per_subcategory = sales_per_subcategory.orderBy('profit_margin', ascending=False)

# Show the result
display(sales_per_subcategory)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.Plot a graph depicting profit margins of different subcategories.

# COMMAND ----------

from pyspark.sql import functions as F
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# Grouping the data by 'category' and 'sub-category' and calculating the sum of 'sales' and 'profit'
sales_per_subcategory = df1.groupBy('category', 'sub-category').agg(
    F.sum('sales').alias('sales'),
    F.sum('profit').alias('profit')
)

# Create a new column for 'profit_margin' as profit divided by sales
sales_per_subcategory = sales_per_subcategory.withColumn(
    'profit_margin', 
    F.col('profit') / F.col('sales')
)

# Converting to Pandas DataFrame for plotting
sales_per_subcategory_pd = sales_per_subcategory.toPandas()

# Sorting the dataframe by 'profit_margin' in descending order
sales_per_subcategory_pd = sales_per_subcategory_pd.sort_values(by='profit_margin', ascending=False)

# Plotting the profit margins sub-category bar chart
fig, ax = plt.subplots(figsize=(14, 10))

# Plotting the profit margin per sub-category
sns.barplot(
    y=sales_per_subcategory_pd['sub-category'], 
    x=sales_per_subcategory_pd['profit_margin'], 
    hue=sales_per_subcategory_pd['category'], 
    order=['Paper', 'Labels', 'Envelopes', 'Accessories', 'Copiers', 'Binders', 'Art', 'Appliances', 'Fasteners', 
           'Phones', 'Furnishings', 'Bookcases', 'Storage', 'Chairs', 'Supplies', 'Machines', 'Tables'],
    alpha=0.75, dodge=False, ax=ax
)

# Clean out bar junk
ax.spines['left'].set_position('zero')
ax.spines[['right', 'top']].set_visible(False)
ax.set(ylabel=None, xlabel='Profit Margin (%)')

def move_ylabel_tick(index: list):
    """Moving the provided ylabel ticks to avoid collision with bars."""
    for tick in index:
        ax.get_yticklabels()[tick].set_x(0.02)
        ax.get_yticklabels()[tick].set_horizontalalignment('left')

# Move the y-labels for sub-categories that are making a loss to prevent text-bar collision
move_ylabel_tick([-1, -2, -3])

# Annotating the profit margin amount for each bar
for p in ax.patches:
    _, y = p.get_xy()
    ax.annotate(f'{p.get_width() * 100:.1f}%', (p.get_width() / 2, y + 0.45))

# Calculating Superstore's aggregate profit margin
mean_profit = sales_per_subcategory_pd['profit'].sum() / sales_per_subcategory_pd['sales'].sum()

# Plotting a vertical line for Superstore's aggregate profit margin
ax.axvline(mean_profit, color='red', label='Mean Profit, All Categories', alpha=0.75, ls='--')

# Setting the title and legend
ax.set_title('Profit Margin by Sub-category', fontdict={'fontsize': 18})
ax.legend(loc=(1, 0.9))

# Formatting the x-axis as percentage
ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC From the analysis, only one sub category is making a loss. That is tables subcategory from the furniture category. This explains the low profit percentages from the furniture category despite high sales.
# MAGIC
# MAGIC Mean profit margin is at 11.6 % The top three highest profit margins are in the office supplies sector. 11 out of 17 subsectors have profit margins above the mean profit margin.

# COMMAND ----------

# MAGIC %md
# MAGIC Investigate products in the tables sub category that are not doing well

# COMMAND ----------

# Grouping the data by the relevant columns (product_name, row_id, postal_code)
grouped_data = df1.groupBy(['product_name', 'row_id', 'postal_code']).sum()

# Calculate the profit margin (Profit / Sales) for each group
grouped_data = grouped_data.withColumn('profit_margin', grouped_data['sum(profit)'] / grouped_data['sum(sales)'])

# Sorting the data based on the profit margin (ascending or descending)
grouped_data = grouped_data.orderBy('profit_margin', ascending=False)

# Show the resulting DataFrame
display(grouped_data)

# COMMAND ----------

from pyspark.sql.functions import col

# Convert 'profit' and 'sales' columns to numeric types
df1 = df1.withColumn('profit', col('profit').cast('double'))
df1 = df1.withColumn('sales', col('sales').cast('double'))

# Filter the DataFrame for the 'Tables' sub-category
tables = df1.filter(df1['sub-category'] == 'Tables')

# Group by 'product_name' and calculate the sum of 'profit' and 'sales'
tables = tables.groupBy('product_name').sum('profit', 'sales')

# Calculate the profit margin
tables = tables.withColumn('profit_margin', tables['sum(profit)'] / tables['sum(sales)'])

# Sort the DataFrame by the highest profit margins
tables = tables.orderBy('profit_margin', ascending=False)

# Display the DataFrame
display(tables)

# COMMAND ----------

# Filter the tables DataFrame to only show rows where sales are not zero
table_sales = tables[tables['sum(sales)'] != 0]

# Display the resulting DataFrame
display(table_sales)

# COMMAND ----------

# Calculate the company-wide average profit margin
mean_profit = (sales_per_subcategory.agg({"profit": "sum"}).collect()[0][0] / 
               sales_per_subcategory.agg({"sales": "sum"}).collect()[0][0])

# Function to check how many items have a profit margin higher than the company average
def profitable_items(category_dict: dict):
    """
    Function to print the amount of items that have a higher profit margin than the company average
    """
    for df_name, df in category_dict.items():
        # Filtering items with profit margin higher than mean_profit
        profitable_items = df.filter(df['profit_margin'] > mean_profit)
        print(f"""{df_name} has {profitable_items.count()} items (out of {df.count()}) with a profit margin higher than the company average.""")

# Call the function for the "Tables" category
profitable_items({"Tables": table_sales})

# COMMAND ----------

# MAGIC %md
# MAGIC # Segment Analysis

# COMMAND ----------

from pyspark.sql.functions import sum

# Group by 'segment' and compute sum for each column
sales_by_segment = df1.groupBy("segment").agg(
    sum("row_id").alias("total_row_id"),
    sum("postal_code").alias("total_postal_code"),
    sum("sales").alias("total_sales"),
    sum("quantity").alias("total_quantity"),
    sum("discount").alias("total_discount"),
    sum("profit").alias("total_profit"),
    sum("shipping_cost").alias("total_shipping_cost"),
    sum("sales_year").alias("total_sales_year")
)

# Show the result
display(sales_by_segment)


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Grouping by segment and aggregating other columns as needed
segment_sales = df1.groupBy("segment").agg(
    F.sum("sales").alias("total_sales"),
    F.sum("quantity").alias("total_quantity"),
    F.sum("discount").alias("total_discount"),
    F.sum("profit").alias("total_profit"),
    F.sum("shipping_cost").alias("total_shipping_cost"),
    F.first("sales_year").alias("sales_year")  # Assuming year is the same for all rows in each segment
)

# Converting to Pandas for better visualization and handling
segment_sales_pd = segment_sales.toPandas()

# Sorting values for better visualization (optional)
segment_sales_pd = segment_sales_pd.sort_values(by="total_sales", ascending=False)

# Display the aggregated sales data by segment
display(segment_sales_pd)


# COMMAND ----------

# Pie chart
plt.figure(figsize=(12,10))
plt.pie(segment_sales_pd["total_sales"], 
        labels=segment_sales_pd["segment"], 
        startangle=90, 
        counterclock=False,
        wedgeprops={'linewidth':1, 'edgecolor':'black', 'alpha':1},
        autopct=lambda p: f'{p: .1f}% \n ${p*np.sum(segment_sales_pd.total_sales)/100 :,.0f}')

plt.title("Share of Sales per Segment", fontdict={'fontsize':18})
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Group by segment and aggregate the necessary columns
segment_sales = df1.groupBy("segment").agg(
    F.sum("sales").alias("total_sales"),
    F.sum("quantity").alias("total_quantity"),
    F.sum("profit").alias("total_profit"),
    F.sum("shipping_cost").alias("total_shipping_cost")
)

# Calculate profit margin as profit/sales
segment_sales = segment_sales.withColumn("profit_margin", 
                                         F.col("total_profit") / F.col("total_sales"))

# Convert the aggregated result to a Pandas DataFrame for easier plotting
segment_sales_pd = segment_sales.toPandas()

# Subset the columns for plotting (segment, profit, profit_margin)
segment_sales_pd = segment_sales_pd[['segment', 'total_quantity', 'total_profit', 'profit_margin']]

# Sorting the data by profit margin for better visualization
segment_sales_pd = segment_sales_pd.sort_values(by='profit_margin', ascending=False)

# Plot a bar chart for profit margins per segment
plt.figure(figsize=(10, 6))
bars = plt.bar(segment_sales_pd['segment'], segment_sales_pd['profit_margin'] * 100, color=['skyblue', 'lightgreen', 'salmon'])

# Add titles and labels
plt.title('Profit Margin by Segment', fontsize=16)
plt.xlabel('Segment', fontsize=14)
plt.ylabel('Profit Margin (%)', fontsize=14)

# Annotate the bars with the profit margin percentages
for bar in bars:
    yval = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, yval + 0.5, f'{yval:.1f}%', ha='center', va='bottom', fontsize=12)

# Show the bar chart
plt.tight_layout()
plt.show()

# Display the data in the new format
display(segment_sales_pd)


# COMMAND ----------

# MAGIC %md
# MAGIC Among the segments, Home Office has the highest profit margin percent. Both consumer and corporate segments have equal percent margins of 11.5%

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql.functions import sum as _sum

# Group by segment and category, summing sales
segment_category = df1.groupBy('segment', 'category').agg(_sum('sales').alias('sales')).toPandas()

# Plot a bar plot of sales by category per segment, using category as the hue
sns.catplot(
    data=segment_category,
    x='segment',
    y='sales',
    kind='bar',
    hue='category',
    height=6,
    aspect=1.5
)

# Set the title and axis labels
plt.title('Sales by Category Per Segment', fontsize=15)
plt.xlabel('Segment', fontsize=12)
plt.ylabel('Sales in Millions', fontsize=12)

# Formatting for better readability
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for better visibility
plt.tight_layout()  # Adjust layout to prevent overlap

# Show the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC The consumer segment contributes the highest revenue, then corporate and lastly home office. In each of the three segments, Technology leads in sales followed by furniture and finally office supplies .

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geographical Market Location Analysis

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, count

# Count of unique countries
display(df1.select(countDistinct(col('Country'))))

# Count of unique cities
display(df1.select(countDistinct(col('City'))))

# Top 10 cities with relative frequencies
city_counts = df1.groupBy('City').count()
city_counts = city_counts.withColumn('relative_frequency', col('count') / df1.count())
display(city_counts.orderBy(col('relative_frequency').desc()).limit(10))

# Sum of the relative frequencies of the top 10 cities
top_10_cities = city_counts.orderBy(col('relative_frequency').desc()).limit(10)
display(top_10_cities.agg({'relative_frequency': 'sum'}))

# COMMAND ----------


import pandas as pd
from pyspark.sql.functions import col

# Calculate the percentage of customers per city
city_counts = df1.groupBy('City').count()
city_counts = city_counts.withColumn('% of Customers', (col('count') / df1.count()) * 100)

# Convert to Pandas DataFrame
city_counts_pd = city_counts.select('City', '% of Customers').toPandas()

# Sort and select top 10 cities
top_cities = city_counts_pd.sort_values('% of Customers', ascending=False).head(10)

display(top_cities)

# COMMAND ----------

from pyspark.sql import functions as F
import seaborn as sns
import matplotlib.pyplot as plt

# Assuming 'df1' is a PySpark DataFrame
# Group by 'City' and calculate the percentage of orders
city_counts = df1.groupBy('City').count()

# Calculate the total number of orders
total_orders = city_counts.agg(F.sum('count')).collect()[0][0]

# Calculate the percentage of orders for each city
city_percentage = city_counts.withColumn('percentage', (city_counts['count'] / total_orders) * 100)

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
city_percentage_pd = city_percentage.toPandas()

# Sort by percentage and get the top 10 cities
top_cities = city_percentage_pd.sort_values('percentage', ascending=False).head(10)

# Plot the bar chart
plt.figure(figsize=(20, 6.5))
h = sns.barplot(data=top_cities, x='City', y='percentage')

# Add annotations for the percentage on top of each bar
for index, row in top_cities.iterrows():
    h.text(index, row['percentage'] + 0.5, f'{row["percentage"]:.2f}%', ha='center', fontsize=19, color='black')

# Customize the plot
plt.xlabel('City', fontsize=18)
plt.ylabel('% of Orders', fontsize=17)
plt.title('Top 10 Cities by Orders', fontsize=20, fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.tick_params(axis='both', labelsize=16)

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Get the count of unique values in the 'Country' column
unique_countries = df1.select('Country').distinct().count()
print(f"Number of unique countries: {unique_countries}")

# Get the value counts and normalize them (percentage)
country_counts = df1.groupBy('Country').count()

# Calculate the total number of rows (orders)
total_rows = df1.count()

# Add a column for the normalized (percentage) value counts
country_percentage = country_counts.withColumn('percentage', (country_counts['count'] / total_rows) * 100)

# Show the top 10 countries by percentage
top_countries = country_percentage.orderBy(F.desc('percentage')).limit(10)
top_countries.show()

# Sum of top 10 countries' percentages (should be close to 100% if only the top 10 are considered)
sum_percentage = top_countries.agg(F.sum('percentage')).collect()[0][0]
print(f"Sum of top 10 countries' percentages: {sum_percentage:.2f}")


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Country' and calculate the count
country_counts = df1.groupBy('Country').count()

# Calculate the total number of rows (customers/orders)
total_rows = df1.count()

# Add a column for the normalized percentage of customers
country_percentage = country_counts.withColumn('percentage', (country_counts['count'] / total_rows) * 100)

# Sort by percentage and get the top 10 countries
top_countries = country_percentage.orderBy(F.desc('percentage')).limit(10)

# Show the results
top_countries.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

# Group by 'Country' and calculate the count
country_counts = df1.groupBy('Country').count()

# Calculate the total number of rows (customers/orders)
total_rows = df1.count()

# Add a column for the normalized percentage of customers
country_percentage = country_counts.withColumn('percentage', (country_counts['count'] / total_rows) * 100)

# Sort by percentage and get the top 10 countries
top_countries = country_percentage.orderBy(F.desc('percentage')).limit(10)

# Convert to Pandas DataFrame for plotting
top_countries_pd = top_countries.toPandas()

# Plot the bar chart
plt.figure(figsize=(20, 6.5))
sns.barplot(data=top_countries_pd, x='Country', y='percentage')

# Add annotations for the percentage on top of each bar
for index, row in top_countries_pd.iterrows():
    plt.text(index, row['percentage'] + 0.5, f'{row["percentage"]:.2f}%', ha='center', fontsize=12, color='black')

# Customize the plot
plt.xlabel('Country', fontsize=18)
plt.ylabel('% of Customers', fontsize=17)
plt.title('Top 10 Countries by Customers', fontsize=20, fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.tick_params(axis='both', labelsize=16)

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Region' and count the occurrences
region_counts = df1.groupBy('Region').count()

# Calculate the total number of rows (customers/orders)
total_rows = df1.count()

# Add a column for the normalized percentage of customers in each region
region_percentage = region_counts.withColumn('percentage', (region_counts['count'] / total_rows) * 100)

# Show the bottom 10 regions by percentage
bottom_10_regions = region_percentage.orderBy('percentage').limit(10)

# Show the sum of the top 10 percentages
top_10_percentage_sum = region_percentage.orderBy(F.desc('percentage')).limit(10).agg(F.sum('percentage')).collect()[0][0]

# Convert to Pandas DataFrame for viewing
bottom_10_regions_pd = bottom_10_regions.toPandas()

# Display the results
display(bottom_10_regions_pd)
display(f"Sum of top 10 regions percentage: {top_10_percentage_sum}%")


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Region' and count the occurrences
region_counts = df1.groupBy('Region').count()

# Calculate the total number of rows (customers/orders)
total_rows = df1.count()

# Add a column for the normalized percentage of customers in each region
region_percentage = region_counts.withColumn('percentage', (region_counts['count'] / total_rows) * 100)

# Sort by percentage and get the top 10 regions
top_regions = region_percentage.orderBy(F.desc('percentage')).limit(10)

# Convert to Pandas DataFrame for easier viewing
top_regions_pd = top_regions.toPandas()

# Display the top 10 regions with percentage
top_regions_pd


# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import functions as F

# Group by 'Region' and count the occurrences of orders
region_counts = df1.groupBy('Region').count()

# Calculate the total number of orders
total_orders = df1.count()

# Add a column for the normalized percentage of orders in each region
region_percentage = region_counts.withColumn('percentage', (region_counts['count'] / total_orders) * 100)

# Sort by percentage and get the top 10 regions
top_regions = region_percentage.orderBy(F.desc('percentage')).limit(10)

# Convert to Pandas DataFrame for plotting
top_regions_pd = top_regions.toPandas()

# Plot the bar chart
plt.figure(figsize=(20, 6.5))
sns.barplot(data=top_regions_pd, x='Region', y='percentage')

# Add annotations for the percentage on top of each bar
for index, row in top_regions_pd.iterrows():
    plt.text(index, row['percentage'] + 0.5, f'{row["percentage"]:.2f}%', ha='center', fontsize=19, color='black')

# Customize the plot
plt.xlabel('Region', fontsize=18)
plt.ylabel('% of Orders', fontsize=17)
plt.title('Top 10 Regions by Orders', fontsize=20, fontweight='bold')
plt.xticks(rotation=45, ha='right')
plt.tick_params(axis='both', labelsize=16)

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql.functions import col, count, lit

# Group by 'Market' and count occurrences
df_counts = df1.groupBy('Market').count()

# Calculate the total number of rows
total_count = df1.count()

# Add a new column with the normalized counts
df_normalized = df_counts.withColumn('normalized_count', col('count') / lit(total_count))

display(df_normalized)

# COMMAND ----------

from pyspark.sql.functions import col
import matplotlib.pyplot as plt

# Step 1: Calculate the percentage of customers by market
market_counts = df1.groupBy('Market').count()

# Calculate the total number of customers
total_customers = market_counts.agg(F.sum('count')).collect()[0][0]

# Calculate the percentage of customers for each market
market_percentage = market_counts.withColumn('percentage', (col('count') / total_customers) * 100)

# Step 2: Convert to Pandas DataFrame for plotting
market_percentage_pd = market_percentage.toPandas()

# Step 3: Plot the pie chart
plt.figure(figsize=[10, 10])
plt.pie(market_percentage_pd['percentage'], labels=market_percentage_pd['Market'], startangle=90, counterclock=False, 
        wedgeprops={'linewidth': 1, 'edgecolor': 'black', 'alpha': 1}, autopct='%1.1f%%')

plt.title('Share of Customers by Market', fontsize=18)
plt.show()


# COMMAND ----------

from pyspark.sql.functions import col

# Calculate the normalized counts for 'Ship Mode'
ship_mode_counts = df1.groupBy('ship_mode').count()

# Calculate the total count of orders
total_orders = ship_mode_counts.agg(F.sum('count')).collect()[0][0]

# Calculate the percentage for each 'Ship Mode'
ship_mode_percentage = ship_mode_counts.withColumn('percentage', (col('count') / total_orders) * 100)

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
ship_mode_percentage_pd = ship_mode_percentage.toPandas()

# Show the result
display(ship_mode_percentage_pd)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F

# Group by 'Ship Mode' and count the occurrences
ship_mode_counts = df1.groupBy('Ship_Mode').count()

# Calculate the total number of orders
total_orders = ship_mode_counts.agg(F.sum('count')).collect()[0][0]

# Calculate the percentage for each 'Ship Mode'
ship_mode_percentage = ship_mode_counts.withColumn('percentage', (F.col('count') / total_orders) * 100)

# Convert to Pandas DataFrame for plotting
ship_mode_percentage_pd = ship_mode_percentage.toPandas()

# Identify the index of the largest portion (ship mode with the highest percentage)
largest_index = ship_mode_percentage_pd['percentage'].idxmax()

# Create an explode array where the largest portion is pulled out
explode = [0.1 if i == largest_index else 0 for i in range(len(ship_mode_percentage_pd))]

# Plot the pie chart
plt.figure(figsize=(10, 6))
plt.pie(
    ship_mode_percentage_pd['percentage'],
    labels=ship_mode_percentage_pd['Ship_Mode'],
    startangle=90,
    counterclock=False,
    autopct='%1.1f%%',  # Display percentage
    wedgeprops={'linewidth': 1, 'edgecolor': 'black', 'alpha': 0.7},
    explode=explode  # Highlight the largest portion
)
plt.title('Mode of Shipment Ordered', fontsize=15)
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Get the number of unique Customer IDs
unique_customer_count = df1.select('Customer_ID').distinct().count()
print(f"Number of unique customers: {unique_customer_count}")

# Group by 'Customer ID', sum the 'Sales' and 'Quantity', and sort by 'Sales'
top_customers = df.groupBy('Customer ID').agg(
    F.sum('Sales').alias('Total Sales'),
    F.sum('Quantity').alias('Total Quantity')
).orderBy('Total Sales', ascending=False)

# Get the top 1% of customers by sales
top_1_percent_customers = top_customers.limit(int(top_customers.count() * 0.01))

# Show the result
top_1_percent_customers.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Country' and sum 'Sales'
a2 = df1.groupBy('Country').agg(
    F.sum('Sales').alias('Total Sales')
)

# Sort the result by 'Total Sales' in descending order and get top 10
a2_sorted = a2.orderBy('Total Sales', ascending=False).limit(10)

# Show the result
a2_sorted.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 countries generating most sales.

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Country' and sum 'Sales'
a2 = df1.groupBy('Country').agg(
    F.sum('Sales').alias('Sales')
)

# Calculate percentage of sales by country
total_sales = a2.agg(F.sum('Sales')).collect()[0][0]  # Get the total sum of sales
a3 = a2.withColumn('% Sales', (a2['Sales'] / total_sales) * 100)

# Sort by 'Sales' in descending order and limit to top 10
a3_sorted = a3.orderBy('Sales', ascending=False).limit(10)

# Show the result
a3_sorted.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

# Group by 'Country' and sum 'Sales'
a2 = df1.groupBy('Country').agg(
    F.sum('Sales').alias('Sales')
)

# Calculate percentage of sales by country
total_sales = a2.agg(F.sum('Sales')).collect()[0][0]  # Get the total sum of sales
a3 = a2.withColumn('% Sales', (a2['Sales'] / total_sales) * 100)

# Sort by 'Sales' in descending order and limit to top 10
a3_sorted = a3.orderBy('Sales', ascending=False).limit(10)

# Convert PySpark DataFrame to Pandas for plotting
a3_sorted_pd = a3_sorted.toPandas()

# Plot the bar chart
plt.figure(figsize=(10, 6))
sns.barplot(data=a3_sorted_pd, x='Country', y='Sales', palette='viridis')

# Add titles and labels
plt.title('Top 10 Countries by Sales', fontsize=16)
plt.xlabel('Country', fontsize=14)
plt.ylabel('Sales', fontsize=14)
plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for readability

# Display the plot
plt.tight_layout()
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 10 countries based on generated profit.

# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

# Group by 'Country' and sum the 'Profit'
a2 = df1.groupBy('Country').agg(
    F.sum('Profit').alias('Profit')
)

# Calculate the total profit across all countries
total_profit = a2.agg(F.sum('Profit')).collect()[0][0]

# Calculate the profit share for each country
a2 = a2.withColumn('Profit Share', (a2['Profit'] / total_profit) * 100)

# Sort the data by 'Profit Share' in descending order and select the top 10
a3 = a2.orderBy('Profit Share', ascending=False).limit(10)

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
a3_pd = a3.toPandas()

# Plot the bar chart
plt.figure(figsize=(10, 6))
sns.barplot(data=a3_pd, x='Country', y='Profit Share', palette='viridis')

# Add titles and labels
plt.title('Top 10 Countries by Profit Share', fontsize=16)
plt.xlabel('Country', fontsize=14)
plt.ylabel('Profit Share (%)', fontsize=14)
plt.xticks(rotation=45, ha='right')  # Rotate the x-axis labels for readability

# Display the plot
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import seaborn as sns

# Group by 'Market' and sum 'Profit'
a3 = df1.groupBy('Market').agg(F.sum('Profit').alias('Profit'))

# Calculate total profit across all markets
total_profit = a3.agg(F.sum('Profit')).collect()[0][0]

# Calculate profit share for each market
a3 = a3.withColumn('Profit Share', (a3['Profit'] / total_profit) * 100)

# Sort by 'Profit Share' in descending order
a3 = a3.orderBy('Profit Share', ascending=False)

# Convert to Pandas DataFrame for plotting
a4 = a3.toPandas()

# Plot the bar chart
plt.figure(figsize=(10, 6))
sns.barplot(data=a4, x='Market', y='Profit Share', palette='viridis')

# Add titles and labels
plt.title('Profit Share by Market', fontsize=16)
plt.xlabel('Market', fontsize=14)
plt.ylabel('Profit Share (%)', fontsize=14)
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels for readability

# Display the plot
plt.tight_layout()
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F
import matplotlib.pyplot as plt

# Step 1: Group by 'Market' and sum 'Profit'
a3 = df1.groupBy('Market').agg(F.sum('Profit').alias('Profit'))

# Step 2: Calculate total profit across all markets
total_profit = a3.agg(F.sum('Profit')).collect()[0][0]

# Step 3: Calculate profit share for each market
a3 = a3.withColumn('Profit Share', (a3['Profit'] / total_profit) * 100)

# Step 4: Sort by 'Profit Share' in descending order
a3 = a3.orderBy('Profit Share', ascending=False)

# Step 5: Convert to Pandas DataFrame for plotting
a4 = a3.toPandas()

# Step 6: Determine the largest market and create explode array to highlight it
explode = [0.1 if i == 0 else 0 for i in range(len(a4))]  # Explode the first segment (largest)

# Step 7: Plot the pie chart
plt.figure(figsize=(10, 6))
plt.pie(
    a4['Profit Share'], 
    labels=a4['Market'], 
    startangle=90, 
    counterclock=False, 
    autopct='%1.1f%%',  # Display percentage
    wedgeprops={'linewidth': 1, 'edgecolor': 'black', 'alpha': 0.7},  # Wedge properties
    explode=explode  # Highlight the largest portion
)

# Set the title of the plot
plt.title('Profit Share by Market', fontsize=15)

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql.functions import col, datediff

# Assuming 'df' is your PySpark DataFrame with 'ship_date' and 'order_date' columns in string format or date format
df1 = df1.withColumn('shipment_days', datediff(col('ship_date'), col('order_date')))

# Show the first 2 rows
display(df1)

# COMMAND ----------

from pyspark.sql.functions import year, month, col

# Extract the year and month from 'order_date' and 'ship_date' columns
df1 = df1.withColumn('order_year', year(col('order_date')))
df1 = df1.withColumn('order_month', month(col('order_date')))
df1 = df1.withColumn('ship_year', year(col('ship_date')))
df1 = df1.withColumn('ship_month', month(col('ship_date')))

# Show the first 2 rows
display(df1)

# COMMAND ----------

# Group by Order Year and count the orders
order_by_year = df1.groupBy('order_year').count()

# Convert the PySpark DataFrame to Pandas DataFrame for plotting
order_by_year_pd = order_by_year.toPandas()

# Show the first few rows
order_by_year_pd.head()


# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Plotting the bar chart
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=order_by_year_pd, x='order_year', y='count', palette='viridis')

# Add title and labels
plt.title('Orders Placed by Year', fontsize=15)
plt.xlabel('Order Year', fontsize=12)
plt.ylabel('Number of Orders', fontsize=12)

# Annotate the bars with the respective counts
for p in ax.patches:
    ax.annotate(f'{int(p.get_height())}', 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='center', 
                fontsize=12, color='black', 
                xytext=(0, 5), textcoords='offset points')

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Order Year' and sum the 'Sales'
sales_by_year = df1.groupBy('order_year').agg(F.sum('Sales').alias('Total Sales'))

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
sales_by_year_pd = sales_by_year.toPandas()

# Show the first few rows of the result
sales_by_year_pd.head()

import seaborn as sns
import matplotlib.pyplot as plt

# Plotting the bar chart
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=sales_by_year_pd, x='order_year', y='Total Sales', palette='Blues_d')

# Add title and labels
plt.title('Sales by Year', fontsize=15)
plt.xlabel('Order Year', fontsize=12)
plt.ylabel('Total Sales', fontsize=12)

# Annotate the bars with the respective sales values
for p in ax.patches:
    ax.annotate(f'{p.get_height():,.2f}', 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='center', 
                fontsize=12, color='black', 
                xytext=(0, 5), textcoords='offset points')

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Order Year' and sum the 'Profit'
profit_by_year = df1.groupBy('order_year').agg(F.sum('Profit').alias('Total Profit'))

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
profit_by_year_pd = profit_by_year.toPandas()

# Show the first few rows of the result
profit_by_year_pd.head()


import seaborn as sns
import matplotlib.pyplot as plt

# Plotting the bar chart for Profit by Year
plt.figure(figsize=(10, 6))
ax = sns.barplot(data=profit_by_year_pd, x='order_year', y='Total Profit', palette='Blues_d')

# Add title and labels
plt.title('Profit by Year', fontsize=15)
plt.xlabel('Order Year', fontsize=12)
plt.ylabel('Total Profit', fontsize=12)

# Annotate the bars with the respective profit values
for p in ax.patches:
    ax.annotate(f'{p.get_height():,.2f}', 
                (p.get_x() + p.get_width() / 2., p.get_height()), 
                ha='center', va='center', 
                fontsize=12, color='black', 
                xytext=(0, 5), textcoords='offset points')

# Show the plot
plt.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Order Year' and aggregate sum of 'Sales' and 'Profit'
sales_profit_by_year = df1.groupBy('order_year').agg(
    F.sum('Sales').alias('Total Sales'),
    F.sum('Profit').alias('Total Profit')
)

# Convert the PySpark DataFrame to a Pandas DataFrame for plotting
sales_profit_by_year_pd = sales_profit_by_year.toPandas()

# Show the first few rows of the result
sales_profit_by_year_pd.head()


import seaborn as sns
import matplotlib.pyplot as plt

# Plotting the line chart for Sales and Profit by Year
plt.figure(figsize=(12, 6))

# Plot Total Sales as a line
sns.lineplot(data=sales_profit_by_year_pd, x='order_year', y='Total Sales', label='Sales', color='blue', linewidth=2)

# Plot Total Profit as a line
sns.lineplot(data=sales_profit_by_year_pd, x='order_year', y='Total Profit', label='Profit', color='green', linewidth=2)

# Add title and labels
plt.title('Sales and Profit by Year', fontsize=15)
plt.xlabel('Order Year', fontsize=12)
plt.ylabel('Amount (in $)', fontsize=12)

# Add a legend to distinguish between Sales and Profit
plt.legend(title='Metrics', fontsize=12)

# Show the plot
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Country' and count the 'Customer ID' for each country
Orders_Country = df1.groupBy('Country').agg(F.count('customer_id').alias('OrderCount'))

# Show the result
Orders_Country.show()


# COMMAND ----------

from pyspark.sql import functions as F

# Group by 'Country' and calculate the distinct count of 'Customer ID'
Cust_Country = df1.groupBy('Country').agg(F.countDistinct('customer_id').alias('CustomerCount'))

# Show the result
Cust_Country.show()


# COMMAND ----------

# Perform a left join between Orders_Country and Cust_Country on 'Country'
left_join = Orders_Country.join(Cust_Country, on='Country', how='left')

# Show the first 3 rows of the resulting DataFrame
left_join.show(3)


# COMMAND ----------

# MAGIC %md
# MAGIC Demand per 1000 Customers in each Country.
# MAGIC

# COMMAND ----------

#Demand per 1000 Customers in each Country.
from pyspark.sql.functions import col

# Calculate Demand per 1000 Customers in each Country
left_join = left_join.withColumn('Demand', (col('OrderCount') / col('CustomerCount')) * 1000)

# Sort by Demand in descending order and get the top 10 countries
d5 = left_join.orderBy(col('Demand').desc()).limit(10)

# Show the result
d5.show()


# COMMAND ----------

df1.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering 

# COMMAND ----------

from pyspark.sql.functions import col

features_df = df1.select(
    "Sales", "Profit", "Quantity", "Discount", "shipment_days",
    "Segment", "Category", "state", "country", "Sub-Category", "Region"
)

# Drop nulls (for simplicity)
features_df = features_df.na.drop()


# COMMAND ----------

from pyspark.ml.feature import StringIndexer

indexers = [
    StringIndexer(inputCol="Segment", outputCol="Segment_idx"),
    StringIndexer(inputCol="Category", outputCol="Category_idx"),
    StringIndexer(inputCol="Sub-Category", outputCol="SubCategory_idx"),
    StringIndexer(inputCol="Region", outputCol="Region_idx"),
    StringIndexer(inputCol="Country", outputCol="Country_idx"),
    StringIndexer(inputCol="State", outputCol="State_idx")
]


# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=[
        "Profit", "Quantity", "Discount", "shipment_days",
        "Segment_idx", "Category_idx", "SubCategory_idx", "Region_idx",
        "Country_idx", "State_idx"
    ],
    outputCol="features"
)


# COMMAND ----------

from pyspark.ml import Pipeline

# Create the pipeline with the new indexers and assembler
pipeline = Pipeline(stages=indexers + [assembler])

# Transform the data
model_ready_df = pipeline.fit(features_df).transform(features_df)

# Show the first 5 rows of the prepared data
model_ready_df.select("features", "Sales").show(5, truncate=False)


# COMMAND ----------

# Split data into train and test sets
train_data, test_data = model_ready_df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Forest

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Train the Random Forest model
rf = RandomForestRegressor(featuresCol="features", labelCol="Sales", maxBins=1100)
rf_model = rf.fit(train_data)

# Make predictions
predictions = rf_model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

# Evaluate the model using R-squared
evaluator_r2 = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"R-squared for Random Forest: {r2:.2f}")

# Show predictions
predictions.select("Sales", "prediction").show(5)


# COMMAND ----------

# MAGIC %md
# MAGIC ### GBT Regressor

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Train the Gradient Boosting model
gb_regressor = GBTRegressor(featuresCol="features", labelCol="Sales", maxBins=1100)

# Fit the model on the training data
gb_model = gb_regressor.fit(train_data)

# Make predictions on the test data
predictions_gb = gb_model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_gb)
print(f"Root Mean Squared Error (RMSE) for Gradient Boosting: {rmse:.2f}")

# Evaluate the model using R-squared
evaluator_r2 = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions_gb)
print(f"R-squared for Gradient Boosting: {r2:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### XGBoost

# COMMAND ----------

# You can install XGBoost via PyPI
!pip install xgboost


# COMMAND ----------

import xgboost as xgb
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
from sklearn.metrics import mean_squared_error, r2_score

# Convert the DataFrame into a format that XGBoost understands (convert to numpy arrays)
train_data_pd = train_data.select("features", "Sales").toPandas()
X_train = train_data_pd["features"].apply(lambda x: x.toArray()).tolist()
y_train = train_data_pd["Sales"].values

test_data_pd = test_data.select("features", "Sales").toPandas()
X_test = test_data_pd["features"].apply(lambda x: x.toArray()).tolist()
y_test = test_data_pd["Sales"].values

# Convert data to DMatrix format (XGBoost format)
dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

# Set XGBoost parameters
params = {
    'objective': 'reg:squarederror',  # Regression task
    'max_depth': 6,
    'learning_rate': 0.1,
    'n_estimators': 100,
    'eval_metric': 'rmse'
}

# Train the XGBoost model
xgb_model = xgb.train(params, dtrain, num_boost_round=100)

# Make predictions
y_pred = xgb_model.predict(dtest)

# Calculate RMSE and R-squared
rmse_xgb = mean_squared_error(y_test, y_pred, squared=False)
r2_xgb = r2_score(y_test, y_pred)

print(f"Root Mean Squared Error (RMSE) for XGBoost: {rmse_xgb:.2f}")
print(f"R-squared for XGBoost: {r2_xgb:.2f}")


# COMMAND ----------

import pandas as pd
import numpy as np

# Create a DataFrame to compare actual and predicted sales
comparison_xgb_df = pd.DataFrame({
    'Actual_Sales': y_test.flatten(),
    'Predicted_Sales': y_pred.flatten()
})

# Calculate Absolute Error and Percentage Error
comparison_xgb_df['Absolute_Error'] = (comparison_xgb_df['Actual_Sales'] - comparison_xgb_df['Predicted_Sales']).abs()
comparison_xgb_df['Percentage_Error'] = (comparison_xgb_df['Absolute_Error'] / comparison_xgb_df['Actual_Sales']) * 100

# Show a sample
print(comparison_xgb_df.head())


# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_percentage_error

# Assume these are your actual test values
y_test = np.array([...])  # replace [...] with your actual y_test array

# And your predictions from different models
lgb_predictions = np.array([...])  # LightGBM predictions
xgb_predictions = np.array([...])  # XGBoost predictions
prophet_predictions = np.array([...])  # Prophet predictions (align to test set)

# Function to compute RMSE, R2, MAPE
def evaluate_model(y_true, y_pred):
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    r2 = r2_score(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred) * 100  # in %
    return rmse, r2, mape

# Evaluate models
lgb_rmse, lgb_r2, lgb_mape = evaluate_model(y_test, lgb_predictions)
xgb_rmse, xgb_r2, xgb_mape = evaluate_model(y_test, xgb_predictions)
prophet_rmse, prophet_r2, prophet_mape = evaluate_model(y_test, prophet_predictions)

# Create results dataframe
results_df = pd.DataFrame({
    'Model': ['LightGBM', 'XGBoost', 'Prophet'],
    'RMSE': [lgb_rmse, xgb_rmse, prophet_rmse],
    'R²': [lgb_r2, xgb_r2, prophet_r2],
    'MAPE (%)': [lgb_mape, xgb_mape, prophet_mape]
})

# Format and display
results_df = results_df.round(2)
print(results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Linear Regression

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

# Assuming 'features' is the feature column and 'sales' is the target variable
lr = LinearRegression(featuresCol="features", labelCol="Sales")
lr_model = lr.fit(train_data)
lr_predictions = lr_model.transform(test_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="sales", predictionCol="prediction", metricName="rmse")
lr_rmse = evaluator.evaluate(lr_predictions)
print(f"RMSE for Linear Regression: {lr_rmse:.2f}")

# Evaluate the model using R-squared
evaluator_r2 = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="r2")
lr_r2 = evaluator_r2.evaluate(lr_predictions)
print(f"R-squared for Gradient Boosting: {lr_r2:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### LightBGM

# COMMAND ----------

# MAGIC %pip install lightgbm

# COMMAND ----------

import lightgbm as lgb
import numpy as np
from sklearn.metrics import mean_squared_error, r2_score

# Ensure X_train, X_test, y_train, y_test are defined and in the correct format
# For example, if they are pandas DataFrames, convert them to numpy arrays
X_train = X_train.values if hasattr(X_train, 'values') else np.array(X_train)
X_test = X_test.values if hasattr(X_test, 'values') else np.array(X_test)
y_train = y_train.values if hasattr(y_train, 'values') else np.array(y_train)
y_test = y_test.values if hasattr(y_test, 'values') else np.array(y_test)

# Prepare the data
train_data = lgb.Dataset(X_train, label=y_train)
test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)

# Train the model
params = {
    'objective': 'regression',
    'metric': 'rmse',
    'boosting_type': 'gbdt'
}
lgb_regressor = lgb.train(params, train_data, valid_sets=[test_data], num_boost_round=100)

# Make predictions
lgb_predictions = lgb_regressor.predict(X_test)

# Evaluate
lgb_rmse = np.sqrt(mean_squared_error(y_test, lgb_predictions))
lgb_r2 = r2_score(y_test, lgb_predictions)

print(f"Root Mean Squared Error (RMSE) for LightGBM: {lgb_rmse:.2f}")
print(f"R-squared for LightGBM: {lgb_r2:.2f}")

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs, col

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Create a pandas DataFrame to compare actual and predicted sales
comparison_df = pd.DataFrame({
    'Actual_Sales': y_test.flatten(),
    'Predicted_Sales': lgb_predictions.flatten()
})

# Convert pandas DataFrame to Spark DataFrame
comparison_sdf = spark.createDataFrame(comparison_df)

# Calculate Absolute Error and Percentage Error
comparison_sdf = comparison_sdf.withColumn(
    'Absolute_Error', 
    abs(col('Actual_Sales') - col('Predicted_Sales'))
).withColumn(
    'Percentage_Error', 
    (col('Absolute_Error') / col('Actual_Sales')) * 100
)

# Show a sample
display(comparison_sdf.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decision Tree

# COMMAND ----------

from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator

# Initialize the Decision Tree Regressor with a higher maxBins value
dt_regressor = DecisionTreeRegressor(featuresCol="features", labelCol="Sales", maxBins=1100)  # Adjust maxBins as needed

# Fit the model to the training data
dt_model = dt_regressor.fit(train_data)

# Make predictions on the test data
predictions_dt = dt_model.transform(test_data)

# Evaluate the model using RMSE
evaluator = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions_dt)
print(f"Root Mean Squared Error (RMSE) for Decision Tree: {rmse:.2f}")

# Evaluate the model using R-squared
evaluator_r2 = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions_dt)
print(f"R-squared for Decision Tree: {r2:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### SVM

# COMMAND ----------

from pyspark.ml.regression import GBTRegressor
from pyspark.ml.feature import VectorAssembler

from sklearn.svm import SVR
from sklearn.metrics import mean_squared_error, r2_score

# Convert Spark DataFrame to Pandas DataFrame
train_pd = train_data.select("features", "Sales").toPandas()
test_pd = test_data.select("features", "Sales").toPandas()

# Separate features and labels
X_train = np.array(train_pd["features"].apply(lambda x: x.toArray()).tolist())
y_train = train_pd["Sales"].values
X_test = np.array(test_pd["features"].apply(lambda x: x.toArray()).tolist())
y_test = test_pd["Sales"].values

# Fit the SVM model
svm = SVR(kernel='rbf')
svm.fit(X_train, y_train)

# Make predictions
predictions_svm = svm.predict(X_test)

# Evaluate performance
svm_rmse = np.sqrt(mean_squared_error(y_test, predictions_svm))
svm_r2 = r2_score(y_test, predictions_svm)

print(f"Root Mean Squared Error (RMSE) for SVM: {svm_rmse:.2f}")
print(f"R-squared for SVM: {svm_r2:.2f}")


# COMMAND ----------

# Convert the features column from DenseVector to NumPy array
train_pd["features"] = train_pd["features"].apply(lambda x: np.array(x))  # Convert DenseVector to NumPy array
test_pd["features"] = test_pd["features"].apply(lambda x: np.array(x))

# Now separate the features and target variable
X_train = np.array(train_pd["features"].tolist())  # Convert the list of vectors into a NumPy array
y_train = train_pd["Sales"].values  # Target variable
X_test = np.array(test_pd["features"].tolist())
y_test = test_pd["Sales"].values


# COMMAND ----------

from sklearn.ensemble import AdaBoostRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_squared_error, r2_score

# Define the base regressor (a shallow decision tree)
base_regressor = DecisionTreeRegressor(max_depth=3)

# Initialize AdaBoost Regressor
ada_boost = AdaBoostRegressor(base_estimator=base_regressor, n_estimators=50, random_state=42)

# Fit the model on the training data
ada_boost.fit(X_train, y_train)

# Make predictions on the test data
y_pred = ada_boost.predict(X_test)

# Evaluate the model's performance using RMSE and R-squared
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)

print(f"Root Mean Squared Error (RMSE) for AdaBoost: {rmse:.2f}")
print(f"R-squared for AdaBoost: {r2:.2f}")


# COMMAND ----------

# Evaluate the model's performance
evaluator = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse:.2f}")

# Alternatively, evaluate R²
evaluator_r2 = RegressionEvaluator(labelCol="Sales", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"R-squared: {r2:.2f}")


# COMMAND ----------

from pyspark.sql.functions import log, col

# Add LogSales column
features_df = features_df.withColumn("LogSales", log(col("Sales") + 1))  # +1 to avoid log(0)

# Split the data AFTER creating LogSales
train_data, test_data = features_df.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor

# Initialize the model with LogSales as the target
rf = RandomForestRegressor(
    featuresCol="features", 
    labelCol="LogSales",  # Use LogSales instead of Sales
    maxBins=1100
)

# COMMAND ----------

print("Columns in train_data:", features_df.columns)

# COMMAND ----------

from pyspark.sql.functions import exp

predictions = predictions.withColumn("PredictedSales", exp(col("prediction")) - 1)

# COMMAND ----------

from pyspark.sql.functions import log, col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Add LogSales column
df1 = df1.withColumn("LogSales", log(col("Sales") + 1))

# 2. Define feature columns (ensure these exist in the DataFrame)
feature_cols = ["order_month", "order_year", "Quantity", "Sales Lag 1", "Sales Rolling Avg 3M", "Is Holiday", "Category Index", "RFM Score"]

# 3. Split data into train/test
train_data, test_data = features_df.randomSplit([0.8, 0.2], seed=42)

# 4. Initialize assembler and model
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
rf = RandomForestRegressor(labelCol="LogSales", featuresCol="features", maxBins=1100)

# 5. Create pipeline
pipeline = Pipeline(stages=[assembler, rf])

# 6. Hyperparameter tuning
param_grid = (ParamGridBuilder()
              .addGrid(rf.numTrees, [50, 100])
              .addGrid(rf.maxDepth, [5, 10])
              .build())

evaluator = RegressionEvaluator(labelCol="LogSales", metricName="rmse")
crossval = CrossValidator(estimator=pipeline, estimatorParamMaps=param_grid, evaluator=evaluator, numFolds=3)

# 7. Train the model
cv_model = crossval.fit(train_data)
best_model = cv_model.bestModel

# 8. Predict and evaluate
predictions = best_model.transform(test_data)
predictions.select("LogSales", "prediction").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC now for the Data Ware house
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum

# Group by 'Order Date' and sum 'Sales'
d3 = df1.groupBy('order_date').agg(_sum('Sales').alias('Total_Sales'))

# Show top 10 rows
d3.show(10)


# COMMAND ----------

from pyspark.sql.functions import year, month, sum as _sum, to_date, date_format

# Convert 'order_date' to date type and extract month
df1 = df1.withColumn("order_date", to_date(df1["order_date"], "dd-MM-yyyy"))
df1 = df1.withColumn("YearMonth", date_format(df1["order_date"], "yyyy-MM"))

# Group by 'YearMonth' and sum 'Sales'
d3 = df1.groupBy("YearMonth").agg(_sum("Sales").alias("Total_Sales"))

# Sort by 'YearMonth'
d3 = d3.orderBy("YearMonth")

# Show first 15 months
display(d3.limit(15))

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

# Convert Spark DataFrame to pandas DataFrame
d3_pd = d3.toPandas()

# Convert 'YearMonth' to datetime if it's still a string
d3_pd['YearMonth'] = pd.to_datetime(d3_pd['YearMonth'])

# Sort by date to ensure correct plotting
d3_pd = d3_pd.sort_values(by='YearMonth')

# Plotting
sns.lineplot(data=d3_pd, x='YearMonth', y='Total_Sales', ci=None)
plt.gcf().set_size_inches(17, 8)
sns.set(rc={'axes.facecolor':'#c5b5d4','figure.facecolor':'#04f5e1'})
plt.xlabel('Date', fontsize=19, fontweight='bold')
plt.ylabel('Sales', fontsize=18, fontweight='bold')
plt.title('Actual Sales', fontsize=19, fontweight='bold')
plt.tick_params(axis='both', labelsize=15.8, length=6, width=2, colors='#300923',
                grid_color='#300923', grid_alpha=0.5)
plt.show()


# COMMAND ----------

import statsmodels.api as sm
from statsmodels.tsa.seasonal import seasonal_decompose
import pandas as pd

# COMMAND ----------

# Make sure your DataFrame is indexed by datetime
d3_pd.set_index('YearMonth', inplace=True)
d3_pd.index = pd.to_datetime(d3_pd.index)

# Decompose the 'Total_Sales' series
s1 = seasonal_decompose(d3_pd['Total_Sales'], model='multiplicative', period=12)

# Plot decomposition results
s1.plot()
sns.set(rc={'axes.facecolor':'#c5b5d4','figure.facecolor':'#04f5e1'})
plt.gcf().set_size_inches(19, 9)
plt.show()


# COMMAND ----------

# Split using iloc (if you want first 36 rows)
d_train = d3_pd.iloc[:36]
print(d_train.tail())

d_test = d3_pd.iloc[36:]
print(d_test.head())


# COMMAND ----------

from statsmodels.tsa.holtwinters import ExponentialSmoothing

# Fit Holt-Winters model
hwmodel = ExponentialSmoothing(
    d_train['Total_Sales'],
    trend='mul',
    seasonal='mul',
    seasonal_periods=12
).fit()


# COMMAND ----------

forecast = hwmodel.forecast(len(d_test))


# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(18, 8))
plt.plot(d_train.index, d_train['Total_Sales'], label='Train', color='blue')
plt.plot(d_test.index, d_test['Total_Sales'], label='Test', color='green')
plt.plot(d_test.index, forecast, label='Forecast', color='red')
plt.xlabel('Date', fontsize=18)
plt.ylabel('Sales', fontsize=18)
plt.title('Holt-Winters Forecast', fontsize=20)
plt.legend(fontsize=15)
plt.grid(True)
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt

plt.figure(figsize=(18, 6))
plt.plot(d_train.index, d_train['Total_Sales'], label='Train')
plt.plot(d_test.index, d_test['Total_Sales'], label='Test')
plt.plot(d_test.index, forecast, label='Forecast', color='red', linestyle='--')
plt.title('Holt-Winters Forecast')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.legend()
plt.grid(True)
plt.show()


# COMMAND ----------

from sklearn.metrics import mean_squared_error
import numpy as np

# Assign forecast
test_pred = forecast

# RMSE
rmse = np.sqrt(mean_squared_error(d_test['Total_Sales'], test_pred))
print(f"RMSE = {rmse}")

# Mean of all sales data
mean_sales = d3_pd['Total_Sales'].mean()
print(f"MEAN = {mean_sales}")

# Standard deviation of all sales data
std_sales = np.sqrt(d3_pd['Total_Sales'].var())
print(f"SD = {std_sales}")


# COMMAND ----------

r2 = r2_score(d_test['Total_Sales'], test_pred)
print(f"R² = {r2}")

# COMMAND ----------

# Fit the final Holt-Winters model on the entire dataset
final_model = ExponentialSmoothing(
    d3_pd['Total_Sales'], 
    trend='mul', 
    seasonal='mul', 
    seasonal_periods=12
).fit()

# Forecast for the next 12 periods (12 months)
predicted = final_model.forecast(12)

# Print the forecasted values
print(predicted)


# COMMAND ----------

# Plotting the original data and forecast
plt.figure(figsize=(18, 6))
plt.plot(d3_pd.index, d3_pd['Total_Sales'], label='Original Sales')
plt.plot(pd.date_range(d3_pd.index[-1], periods=13, freq='M')[1:], predicted, label='Forecast', color='red', linestyle='--')
plt.title('Holt-Winters Forecast (Next 12 Months)')
plt.xlabel('Date')
plt.ylabel('Sales')
plt.legend()
plt.grid(True)
plt.show()
