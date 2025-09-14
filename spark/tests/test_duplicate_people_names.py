from pyspark.sql import SparkSession

def test_duplicate_people_names():
    spark = SparkSession.builder \
        .appName("DuplicateNames") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_people_data_2k")
    dupes = df.groupBy("name").count().filter("count > 1")
    dupes.show()

    spark.stop()

if __name__ == "__main__":
    test_duplicate_people_names()
