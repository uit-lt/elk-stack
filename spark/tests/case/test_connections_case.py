import unittest
from pyspark.sql import SparkSession

class ConnectionsTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ConnectionsTestCase") \
            .master("local[*]") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
            .getOrCreate()
        cls.df = cls.spark.read.format("org.elasticsearch.spark.sql").load("2_connections_data_300k")
        cls.df.createOrReplaceTempView("connections")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_count_connections(self):
        count = self.df.count()
        self.assertGreater(count, 1000, "Connections index should have at least 1000 records")

    def test_relationship_group_by(self):
        result = self.spark.sql("SELECT relationship, COUNT(*) as cnt FROM connections GROUP BY relationship")
        counts = [row['cnt'] for row in result.collect()]
        self.assertTrue(all(c > 0 for c in counts), "All relationship counts should be greater than 0")

if __name__ == "__main__":
    unittest.main()
