from pyspark.sql import SparkSession

def test_employees():
    spark = SparkSession.builder \
        .appName("TestEmployees") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("employees")
    print("Total employees:", df.count())
    df.show(5)

    result = df.groupBy("code").count()
    result.show()

    spark.stop()

if __name__ == "__main__":
    test_employees()
