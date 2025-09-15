from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("Update5Records") \
        .config("spark.es.port", "9200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def main():
    print("=== UPDATE 5 RECORDS ===")

    # Create Spark Session
    spark = create_spark_session()
    print(f"Application ID: {spark.sparkContext.applicationId}")

    # Define updated records data
    updated_records_data = [
        (24410114, "Tran Trieu Thuan updated", 30),
        (24410100, "Nguyen Phuong Tan updated", 30),
        (24410109, "Nguyen Thi Thu Thao updated", 28),
        (24410092, "Huynh Duy Quoc updated", 35),
        (24410040, "Ha Huy Hung updated", 22)
    ]

    update_ids = [record[0] for record in updated_records_data]
    print(f"Updating records with IDs: {update_ids}")

    # Read current data
    current_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    # Show records before update
    records_before_update = current_df.filter(col("id").isin(update_ids))
    print("\nRecords before update:")
    records_before_update.show()

    # Create DataFrame with updated data
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    updated_df = spark.createDataFrame(updated_records_data, schema)

    print("\nRecords after update:")
    updated_df.show()

    # Write updated records back to Elasticsearch
    print("\nWriting updated records to Elasticsearch...")
    try:
        updated_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "id") \
            .option("es.batch.size.bytes", "10mb") \
            .option("es.batch.size.entries", "1000") \
            .mode("overwrite") \
            .save()
        print("✓ Records updated successfully!")
    except Exception as e:
        print(f"✗ Update failed: {e}")

    # Verify update
    print("\nVerifying update...")
    # Read fresh data to verify the update
    verification_df = spark.read \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .option("es.resource", "2_people_data_2k") \
        .load()

    updated_records = verification_df.filter(col("id").isin(update_ids))
    updated_records.show()

    # Stop Spark session
    spark.stop()
    print("Spark session stopped.")

if __name__ == "__main__":
    main()
