from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create Spark Session
spark = SparkSession.builder \
    .appName("GetAllData") \
    .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
    .getOrCreate()

print("=== 1. GET ALL DATA FROM ELASTICSEARCH ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

try:
    # Read all data from people index
    print("\n--- Reading from 2_people_data_2k index ---")
    people_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    total_count = people_df.count()
    print(f"Total records in 2_people_data_2k: {total_count}")

    print("\nSample data (first 10 records):")
    people_df.select("id", "name", "age").show(10)

    print("\nData summary statistics:")
    people_df.agg(
        min("age").alias("min_age"),
        max("age").alias("max_age"),
        avg("age").alias("avg_age"),
        count("*").alias("total_records")
    ).show()

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")
