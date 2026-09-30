from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("EcommerceFirstJob")
    .getOrCreate()
)

input_path = "hdfs://namenode:8020/data/ecommerce/raw/test_transactions.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("=== SCHEMA ===")
df.printSchema()

print("=== DATA ===")
df.show()

print("=== COUNT ===")
print(df.count())

print("=== REVENUE BY COUNTRY ===")

(
    df.groupBy("country")
    .sum("amount")
    .orderBy("country")
    .show()
)

spark.stop()