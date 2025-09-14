from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import subprocess
import time

# Create Spark Session
spark = SparkSession.builder \
    .appName("Insert5Records") \
    .getOrCreate()

print("=== 2. INSERT 5 RECORDS (id, name, age) ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

# Define schema for new records
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Create 5 new records with specific IDs (8001-8005)
new_records_data = [
    (24410114, "Tran Tireu Thuan", 30),
    (24410100, "Nguyen Phuong Tan", 30),
    (24410109, "Nguyen Thi Thu Thao", 28),
    (24410092, "Huynh Duy Quoc", 35),
    (24410040, "Ha Huy Hung", 22)
]

print("\nRecords to insert:")
new_df = spark.createDataFrame(new_records_data, schema)
new_df.show()

try:
    # Method: Insert via Spark-Elasticsearch connector
    print("\nInserting records via Spark-Elasticsearch connector...")

    # Write DataFrame directly to Elasticsearch using Spark connector
    new_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .option("es.write.operation", "index") \
        .option("es.mapping.id", "id") \
        .mode("append") \
        .save()

    print("✅ Records inserted successfully using Spark connector!")

    # Wait for indexing
    print("\nWaiting 3 seconds for indexing...")
    time.sleep(3)

    # Verify insert by reading back the records
    print("\n--- Verification ---")
    try:
        verification_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
            .load()

        found_count = verification_df.count()
        print(f"Found {found_count} inserted records:")

        if found_count > 0:
            verification_df.select("id", "name", "age").orderBy("id").show()
        else:
            print("⚠️ No records found - may need manual verification")

    except Exception as verify_error:
        print(f"⚠️ Verification error: {verify_error}")
        print("Records may have been inserted but verification failed")

    print("\n✅ INSERT 5 RECORDS - COMPLETED")

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")
