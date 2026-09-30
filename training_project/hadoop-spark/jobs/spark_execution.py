from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from time import sleep

spark = (
    SparkSession.builder
    .appName("SparkExecutionModel")
    .config("spark.sql.files.maxPartitionBytes", 8388608)
    .config("spark.sql.shuffle.partitions", 6)
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

input_path = "hdfs://namenode:8020/data/ecommerce/raw/large_transactions.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("\n=== INPUT PARTITIONS ===")
print(df.rdd.getNumPartitions())

print("\n=== REVENUE BY COUNTRY ===")

result = (
    df.groupBy("country")
      .agg(spark_sum("amount").alias("total_revenue"))
      .alias("total_revenue")
    #   .orderBy("country")
)

result.show()

print("\n=== RESULT PARTITIONS ===")
print(result.rdd.getNumPartitions())

print("\n=== RESULT EXPLAIN ===")
result.explain("formatted")

sleep(300)
spark.stop()