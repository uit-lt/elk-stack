from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("SimpleTest") \
        .master("local[*]") \
        .getOrCreate()

    print("Spark session created successfully!")

    # Tạo DataFrame đơn giản
    data = [("Hello",), ("Spark!",)]
    df = spark.createDataFrame(data, ["word"])
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()
