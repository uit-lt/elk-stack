from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time
import urllib.request
import urllib.error

# Create Spark Session
spark = SparkSession.builder \
    .appName("Delete5Records") \
    .getOrCreate()

print("=== 4. DELETE 5 RECORDS (Just Updated) ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

# Define the IDs of records to delete (same as updated in step 3)
delete_ids = [24410114, 24410100, 24410109, 24410092, 24410040]

print(f"Records to delete: {delete_ids}")

try:
    # Read current state before deletion
    print("\n--- Current state before deletion ---")
    try:
        current_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
            .load()

        current_count = current_df.count()
        print(f"Found {current_count} records to delete:")

        if current_count > 0:
            current_df.select("id", "name", "age").orderBy("id").show()
        else:
            print("⚠️ No records found to delete - may need to run insert/update steps first")

    except Exception as read_error:
        print(f"⚠️ Could not read current records: {read_error}")

    # Get total count before deletion
    print("\n--- Getting total count before deletion ---")
    try:
        all_records_before = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .load()

        total_before = all_records_before.count()
        print(f"Total records in index before deletion: {total_before}")

    except Exception as count_error:
        print(f"⚠️ Could not get total count: {count_error}")
        total_before = "unknown"

    # Delete records using urllib (since Spark doesn't have native delete operation)
    print("\n--- Deleting records via HTTP requests ---")

    deleted_count = 0
    for record_id in delete_ids:
        try:
            # Use Python urllib to delete via Elasticsearch REST API
            url = f"http://elasticsearch:9200/2_people_data_2k/_doc/{record_id}"
            req = urllib.request.Request(url, method='DELETE')

            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    if response.status in [200, 404]:  # 200 = deleted, 404 = not found (already deleted)
                        print(f"✅ Deleted record {record_id}")
                        deleted_count += 1
                    else:
                        print(f"❌ Failed to delete record {record_id} (status: {response.status})")
            except urllib.error.HTTPError as e:
                if e.code == 404:  # Not found (already deleted)
                    print(f"✅ Record {record_id} already deleted or not found")
                    deleted_count += 1
                else:
                    print(f"❌ HTTP error deleting record {record_id}: {e.code}")

        except Exception as delete_error:
            print(f"❌ Error deleting record {record_id}: {delete_error}")

    print(f"Successfully processed {deleted_count} deletion requests")

    # Wait for deletion to complete
    print("\nWaiting 3 seconds for deletion to complete...")
    time.sleep(3)

    # Verify deletions
    print("\n--- Verification of Deletions ---")
    try:
        verification_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
            .load()

        remaining_count = verification_df.count()
        print(f"Records still found after deletion: {remaining_count}")

        if remaining_count == 0:
            print("✅ All target records successfully deleted!")
        else:
            print("⚠️ Some records still exist:")
            verification_df.select("id", "name", "age").orderBy("id").show()

        # Get total count after deletion
        all_records_after = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .load()

        total_after = all_records_after.count()
        print(f"Total records in index after deletion: {total_after}")

        if isinstance(total_before, int):
            actual_deleted = total_before - total_after
            print(f"Actually deleted records: {actual_deleted}")

    except Exception as verify_error:
        print(f"⚠️ Verification error: {verify_error}")

    print(f"\n✅ DELETE 5 RECORDS - COMPLETED")
    print(f"Attempted to delete: {len(delete_ids)} records")
    print(f"Successfully processed: {deleted_count} records")

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")
