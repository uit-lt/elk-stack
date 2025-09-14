from pyspark.sql import SparkSession

def test_name_start_with_text():
    spark = SparkSession.builder \
        .appName("NameStartWithText") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    df = spark.read.format("org.elasticsearch.spark.sql").load("2_people_data_2k")

    df.filter(df.name.startswith("B")).show()

    spark.stop()

if __name__ == "__main__":
    test_name_start_with_text()

# Get name start with text
