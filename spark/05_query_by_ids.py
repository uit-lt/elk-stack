from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("QueryByIds") \
        .master("local[*]") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    print("=== QUERY RECORDS BY ID LIST ===")

    # Create Spark Session
    spark = create_spark_session()
    print(f"Application ID: {spark.sparkContext.applicationId}")

    # List of IDs to query (you can modify this list)
    query_ids = [24410114, 24410100, 24410109, 24410092, 24410040]
    print(f"Searching for IDs: {query_ids}")

    # Read data from Elasticsearch
    df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    print(f"\nTotal records in index: {df.count()}")

    # Filter by IDs
    filtered_df = df.filter(col("id").isin(query_ids))
    found_count = filtered_df.count()

    print(f"Found {found_count} matching records:")

    if found_count > 0:
        # Show all matching records
        filtered_df.orderBy("id").show()

        # Show summary statistics
        print("\nSummary statistics for found records:")
        print(f"  - Age range: {filtered_df.agg(min('age'), max('age')).collect()[0]}")
        print(f"  - Average age: {filtered_df.agg(avg('age')).collect()[0][0]:.1f}")

        # Show which IDs were found vs not found
        found_ids = [row['id'] for row in filtered_df.select('id').collect()]
        not_found_ids = [id for id in query_ids if id not in found_ids]

        print(f"\nFound IDs: {sorted(found_ids)}")
        if not_found_ids:
            print(f"Not found IDs: {sorted(not_found_ids)}")
    else:
        print("No records found with the specified IDs.")

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()
