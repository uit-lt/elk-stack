from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min

def test_people_age_stats():
    spark = SparkSession.builder \
        .appName("PeopleAgeStats") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_people_data_2k")

    df.select(avg("age"), max("age"), min("age")).show()

    spark.stop()

if __name__ == "__main__":
    test_people_age_stats()
