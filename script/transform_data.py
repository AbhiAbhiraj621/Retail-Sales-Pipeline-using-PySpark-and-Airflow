#Pyspark transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, sum as _sum, count
import os

RAW_MERGED = 'raw_data/data/orders_customers.csv'
PROCESSED_DIR = 'data/processed'
PROCESSED_MONTHLY = os.path.join(PROCESSED_DIR, 'monthly_sales')
def transform_data():
os.makedirs(PROCESSED_DIR, exist_ok=True)
spark = SparkSession.builder.appName('RetailETL').getOrCreate()
df = spark.read.options(header=True, inferSchema=True).csv(RAW_MERGED)

df = df.dropna(subset=['order_id', 'customer_id'])
df = df.withColumnRenamed('order_purchase_timestamp', 'purchase_date')

df = df.withColumn('purchase_date', col('purchase_date').cast('timestamp'))

df = df.withColumn('year', year(col('purchase_date')))
df = df.withColumn('month', month(col('purchase_date')))

monthly = df.groupBy('year', 'month').agg(
count('order_id').alias('total_orders'),
_sum('payment_value').alias('total_revenue')
).orderBy('year', 'month')

monthly.coalesce(1).write.mode('overwrite').option('header', True).csv(PROCESSED_MONTHLY)