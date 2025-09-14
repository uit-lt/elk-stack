from pyspark.sql import SparkSession

def test_people():
    spark = SparkSession.builder \
        .appName("TestPeople") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_people_data_2k")
    print("Total people:", df.count())
    df.show(5)

    result = df.groupBy("age").count()
    result.show()

    spark.stop()

if __name__ == "__main__":
    test_people()

#  Get Total people
