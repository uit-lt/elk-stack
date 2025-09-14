from pyspark.sql import SparkSession

def test_top_relationships():
    spark = SparkSession.builder \
        .appName("TopRelationships") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_connections_data_300k")

    df.groupBy("relationship").count().orderBy("count", ascending=False).show(3)

    spark.stop()

if __name__ == "__main__":
    test_top_relationships()

# Get top relationships
