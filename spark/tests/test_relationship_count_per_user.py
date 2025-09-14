from pyspark.sql import SparkSession

def test_relationship_count_per_user():
    spark = SparkSession.builder \
        .appName("RelationshipCountPerUser") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_connections_data_300k")

    df.groupBy("src").count().orderBy("count", ascending=False).show(10)

    spark.stop()

if __name__ == "__main__":
    test_relationship_count_per_user()

# Get relationship count per user
