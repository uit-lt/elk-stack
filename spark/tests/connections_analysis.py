from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ConnectionsAnalysisTest") \
        .config("spark.es.nodes", "elasticsearch") \
        .config("spark.es.port", "9200") \
        .getOrCreate()

    # Đảm bảo tên index đúng, bỏ "_doc" nếu Elasticsearch >7.x
    df = spark.read.format("org.elasticsearch.spark.sql").load("2_connections_data_300k")

    print("Total records in connections_300k data:", df.count())
    df.show(10)

    df.createOrReplaceTempView("connections")
    result = spark.sql("SELECT relationship, COUNT(*) AS cnt FROM connections GROUP BY relationship")
    result.show()

    spark.stop()

if __name__ == "__main__":
    main()
