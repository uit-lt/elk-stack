from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("Insert5Records") \
        .master("local[*]") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    print("=== INSERT 5 RECORDS ===")

    # Create Spark Session
    spark = create_spark_session()
    print(f"Application ID: {spark.sparkContext.applicationId}")

    # Define schema for new records
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Create 5 new records
    new_data = [
        (24410114, "Tran Trieu Thuan", 30),
        (24410100, "Nguyen Phuong Tan", 30),
        (24410109, "Nguyen Thi Thu Thao", 28),
        (24410092, "Huynh Duy Quoc", 35),
        (24410040, "Ha Huy Hung", 22)
    ]

    print("Creating 5 new records:")
    for record in new_data:
        print(f"  ID: {record[0]}, Name: {record[1]}, Age: {record[2]}")

    # Create DataFrame
    new_df = spark.createDataFrame(new_data, schema)
    new_df.show()

    # Insert to Elasticsearch
    print("\nInserting records to Elasticsearch...")
    try:
        new_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.write.operation", "index") \
            .option("es.mapping.id", "id") \
            .option("es.batch.size.bytes", "10mb") \
            .option("es.batch.size.entries", "1000") \
            .mode("append") \
            .save()
        print("✓ Records inserted successfully!")
    except Exception as e:
        print(f"✗ Insert failed: {e}")

    # Verify insertion
    print("\nVerifying insertion...")
    all_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    new_total = all_df.count()
    print(f"Total records after insertion: {new_total}")

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()
