from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import subprocess
import time

# Create Spark Session
spark = SparkSession.builder \
    .appName("Update5Records") \
    .getOrCreate()

print("=== 3. UPDATE 5 RECORDS (Just Inserted) ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

# Define the IDs of records to update (same as inserted in step 2)
update_ids = [24410114, 24410100, 24410109, 24410092, 24410040]

# Define updated data
updated_records_data = [
    (24410114, "Tran Tireu Thuan updated", 30),
    (24410100, "Nguyen Phuong Tan updated", 30),
    (24410109, "Nguyen Thi Thu Thao updated", 28),
    (24410092, "Huynh Duy Quoc updated", 35),
    (24410040, "Ha Huy Hung updated", 22)
]

print("\nRecords to update:")
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

updated_df = spark.createDataFrame(updated_records_data, schema)
updated_df.show()

try:
    # Read current state before update
    print("\n--- Current state before update ---")
    try:
        current_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
            .load()

        current_count = current_df.count()
        print(f"Found {current_count} records to update:")
        if current_count > 0:
            current_df.select("id", "name", "age").orderBy("id").show()
        else:
            print("⚠️ No records found to update - may need to run insert step first")

    except Exception as read_error:
        print(f"⚠️ Could not read current records: {read_error}")

    # Update records via Spark-Elasticsearch connector
    print("\n--- Updating records via Spark-Elasticsearch connector ---")

    # Use upsert operation to update existing documents or create if not exist
    updated_df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .option("es.write.operation", "upsert") \
        .option("es.mapping.id", "id") \
        .mode("append") \
        .save()

    print("✅ Records updated successfully using Spark connector!")

    # Wait for indexing
    print("\nWaiting 3 seconds for indexing...")
    time.sleep(3)

    # Verify updates
    print("\n--- Verification of Updates ---")
    try:
        verification_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
            .load()

        verified_count = verification_df.count()
        print(f"Found {verified_count} updated records:")

        if verified_count > 0:
            verification_df.select("id", "name", "age").orderBy("id").show()

            # Check if updates were successful by looking for "updated" in names
            updated_count = verification_df.filter(col("name").contains("updated")).count()
            print(f"Records with 'updated' in name: {updated_count}")

            if updated_count > 0:
                print("✅ Updates appear successful!")
            else:
                print("⚠️ Updates may not have been applied")
        else:
            print("⚠️ No records found after update")

    except Exception as verify_error:
        print(f"⚠️ Verification error: {verify_error}")

    print("\n✅ UPDATE 5 RECORDS - COMPLETED")

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")
