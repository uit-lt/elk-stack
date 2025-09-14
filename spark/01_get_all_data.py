from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("GetAllData") \
        .master("local[*]") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    print("=== GET ALL DATA FROM ELASTICSEARCH ===")

    # Create Spark Session
    spark = create_spark_session()
    print(f"Application ID: {spark.sparkContext.applicationId}")

    # Read all data from people index
    print("Reading from 2_people_data_2k index...")
    people_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    # Show total count
    total_count = people_df.count()
    print(f"Total records: {total_count}")

    # Show sample data
    print("\nSample data (first 10 records):")
    people_df.show(10)

    # Show age distribution
    print("\nAge distribution:")
    age_dist = people_df.groupBy("age").count().orderBy("age")
    age_dist.show(20)

    # Show name statistics
    print("\nMost common names (top 10):")
    name_stats = people_df.groupBy("name").count().orderBy(col("count").desc())
    name_stats.show(10)

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()
