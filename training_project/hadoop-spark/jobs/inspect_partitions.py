from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("InspectSparkPartitions")
    .config("spark.sql.files.maxPartitionBytes", 8388608)
    .getOrCreate()
)

input_path = "hdfs://namenode:8020/data/ecommerce/raw/large_transactions.csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

print("=== SCHEMA ===")
df.printSchema()

print("=== NUMBER OF SPARK PARTITIONS ===")
print(df.rdd.getNumPartitions())

print("=== NUMBER OF ROWS ===")
print(df.count())

spark.stop()