from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time

# Create Spark Session
spark = SparkSession.builder \
    .appName("QueryByIds") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print("=== 5. QUERY RECORDS BY ID LIST ===")
print(f"Application ID: {spark.sparkContext.applicationId}")

try:
    # --- CONFIGURATION SECTION ---
    # TODO: Update this list with your desired IDs
    query_ids = [24410114, 24410100, 24410109, 24410092, 24410040] # Example IDs - CHANGE THESE

    print(f"Querying for IDs: {query_ids}")

    # Read data from Elasticsearch
    print("\n--- Reading from 2_people_data_2k index ---")
    people_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    total_count = people_df.count()
    print(f"Total records in index: {total_count}")

    # Filter by ID list
    print(f"\n--- Filtering by ID list ---")
    filtered_df = people_df.filter(people_df.id.isin(query_ids))

    found_count = filtered_df.count()
    print(f"Found {found_count} records matching the ID list")

    if found_count > 0:
        print("\nMatching records:")
        # Collect and display results manually for better formatting
        results = filtered_df.select("id", "name", "age").orderBy("id").collect()

        print("+----+------------------+---+")
        print("|id  |name              |age|")
        print("+----+------------------+---+")

        for row in results:
            id_str = str(row.id).ljust(4)
            name_str = str(row.name).ljust(18)
            age_str = str(row.age).ljust(3)
            print(f"|{id_str}|{name_str}|{age_str}|")

        print("+----+------------------+---+")

        # Show some statistics
        print(f"\nStatistics for found records:")
        filtered_df.agg(
            min("age").alias("min_age"),
            max("age").alias("max_age"),
            avg("age").alias("avg_age"),
            count("*").alias("found_records")
        ).show()

        # Show which IDs were not found
        found_ids = [row.id for row in results]
        missing_ids = [id for id in query_ids if id not in found_ids]

        if missing_ids:
            print(f"\nIDs not found in index: {missing_ids}")
        else:
            print(f"\nAll requested IDs were found! ✅")

    else:
        print(f"❌ No records found for IDs: {query_ids}")
        print("Please check if these IDs exist in the index.")

    print("\n✅ QUERY BY IDs - COMPLETED SUCCESSFULLY")

except Exception as e:
    print(f"❌ ERROR: {e}")

finally:
    spark.stop()
    print("Spark session stopped.")
