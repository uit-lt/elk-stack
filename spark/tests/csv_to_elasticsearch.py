#!/usr/bin/env python3
"""
PySpark script to load CSV data to Elasticsearch
Creates index '2_people_data_2k_spark' from '2_people_data_2k.csv'
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def create_spark_session():
    """Create Spark session with Elasticsearch connector"""
    return SparkSession.builder \
        .appName("CSV_to_Elasticsearch") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def define_schema():
    """Define schema for people data"""
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

def main():
    """Main function to load CSV to Elasticsearch"""

    # Create Spark session
    spark = create_spark_session()

    try:
        # CSV file path from mounted data directory (using file:// protocol)
        csv_file_path = "file:///usr/share/logstash/data/2_people_data_2k.csv"

        # Define schema
        schema = define_schema()

        print(f"Reading CSV file: {csv_file_path}")

        # Read CSV with defined schema
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(schema) \
            .csv(csv_file_path)

        # Show sample data
        print("Sample data:")
        df.show(5)
        print(f"Total records: {df.count()}")

        # Elasticsearch configuration
        es_config = {
            "es.nodes": "elasticsearch",
            "es.port": "9200",
            "es.nodes.wan.only": "true",
            "es.nodes.discovery": "false",
            "es.index.auto.create": "true",
            "es.write.operation": "index",
            "es.mapping.id": "id",
            "es.resource": "2_people_data_2k_spark"
        }

        print("Writing data to Elasticsearch...")

        # Write to Elasticsearch
        df.write \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_config) \
            .mode("overwrite") \
            .save()

        print("Data successfully loaded to Elasticsearch index: 2_people_data_2k_spark")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()