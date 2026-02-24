import os, sys, json
os.environ['SPARK_HOME'] = r'C:\spark'
os.environ['HADOOP_HOME'] = r'C:\hadoop'
sys.path.insert(0, os.path.join(os.environ['SPARK_HOME'], 'python'))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, ArrayType  # Fixed: StructField

spark = SparkSession.builder \
    .appName("BitcoinETL") \
    .master("local[*]") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem") \
    .getOrCreate()

# Generate sample Bitcoin JSONL (2 txs)
sample_data = [
    {"tx_hash": "tx1", "block_timestamp": 1231006505, "inputs": [{"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", "value": 50.0}], 
     "outputs": [{"index": 0, "address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "value": 49.5}]},
    {"tx_hash": "tx2", "block_timestamp": 1231006506, "inputs": [{"address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2", "value": 49.5}], 
     "outputs": [{"index": 0, "address": "1CUNEBjYrCn2y1SdiZ9Kptuh7wjhAZW89C", "value": 48.0}]}
]

with open("bitcoin_sample.jsonl", "w") as f:
    for record in sample_data:
        f.write(json.dumps(record) + '\n')

# Schema (fixed StructField)
schema = StructType([
    StructField("tx_hash", StringType()),
    StructField("block_timestamp", LongType()),
    StructField("inputs", ArrayType(StructType([
        StructField("address", StringType()),
        StructField("value", DoubleType())
    ])), True),
    StructField("outputs", ArrayType(StructType([
        StructField("index", LongType()),
        StructField("address", StringType()),
        StructField("value", DoubleType())
    ])), True)
])

print("📥 Reading JSON...")
bronze_df = spark.read.schema(schema).json("bitcoin_sample.jsonl")
bronze_df.write.mode("overwrite").parquet("bronze/")
bronze_df.show(truncate=False)
print("✅ Bronze layer saved")

print("🔄 Silver layer...")
silver_outputs = bronze_df.select(
    to_timestamp(col("block_timestamp")).alias("timestamp"),
    col("tx_hash"),
    explode("outputs").alias("out")
).select("timestamp", "tx_hash", col("out.address").alias("to_address"), col("out.value").alias("amount"))
silver_outputs.write.mode("overwrite").parquet("silver_outputs/")
silver_outputs.show(truncate=False)
print("✅ Silver outputs saved")

print("🏆 Gold layer...")
gold_df = silver_outputs.groupBy(
    col("timestamp").cast("date").alias("date"),
    "to_address"
).agg(spark_sum("amount").alias("total_inflow_btc")).orderBy(col("total_inflow_btc").desc())
gold_df.write.mode("overwrite").parquet("gold/")
gold_df.show(truncate=False)
print("✅ Gold aggregations saved! Check bronze/, silver_outputs/, gold/ folders")

spark.stop()
print("🎉 Full medallion pipeline complete!")
