from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time
import urllib.request
import urllib.error
import json

# Create Spark Session
spark = SparkSession.builder \
    .appName("Delete5Records") \
    .getOrCreate()

print("=== 4. DELETE 5 RECORDS (Just Updated) ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

# Define the IDs of records to delete (same as updated in step 3)
delete_ids = [24410114, 24410100, 24410109, 24410092, 24410040]

print(f"Records to delete: {delete_ids}")

# SAFETY CHECK: Ensure we only have exactly 5 records to delete
if len(delete_ids) != 5:
    raise ValueError(f"❌ SAFETY CHECK FAILED: Expected exactly 5 records to delete, but got {len(delete_ids)}")

print("✅ Safety check passed: Exactly 5 records specified for deletion")

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

    # Delete records using Elasticsearch bulk API (safer approach)
    print("\n--- Deleting records using Bulk API ---")

    # Prepare bulk delete payload
    bulk_payload = ""
    for record_id in delete_ids:
        delete_action = {
            "delete": {
                "_index": "2_people_data_2k",
                "_id": str(record_id)
            }
        }
        bulk_payload += json.dumps(delete_action) + "\n"

    print(f"🔍 Bulk delete payload:\n{bulk_payload}")

    deleted_count = 0
    try:
        # Send bulk delete request
        url = "http://elasticsearch:9200/_bulk"
        req = urllib.request.Request(
            url,
            data=bulk_payload.encode('utf-8'),
            method='POST'
        )
        req.add_header('Content-Type', 'application/x-ndjson')

        with urllib.request.urlopen(req, timeout=30) as response:
            response_data = response.read().decode('utf-8')
            response_json = json.loads(response_data)

            print(f"📄 Bulk API response status: {response.status}")

            # Parse bulk response
            if "items" in response_json:
                for item in response_json["items"]:
                    if "delete" in item:
                        delete_result = item["delete"]
                        doc_id = delete_result.get("_id")
                        status = delete_result.get("status")

                        if status in [200, 404]:  # 200 = deleted, 404 = not found
                            print(f"✅ Successfully processed deletion for ID {doc_id} (status: {status})")
                            deleted_count += 1
                        else:
                            print(f"❌ Failed to delete ID {doc_id} (status: {status})")
            else:
                print(f"⚠️ Unexpected response format: {response_data}")

    except Exception as bulk_error:
        print(f"❌ Bulk delete error: {bulk_error}")

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

            # SAFETY VERIFICATION: Make sure we didn't delete more than expected
            if actual_deleted > 5:
                print(f"⚠️ WARNING: More records deleted than expected! Expected ≤5, but {actual_deleted} were deleted")
            elif actual_deleted == 0:
                print("ℹ️ No records were actually deleted (they might not have existed)")
            else:
                print(f"✅ Safe deletion confirmed: {actual_deleted} records deleted (expected ≤5)")

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
