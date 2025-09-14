import unittest
from pyspark.sql import SparkSession

class ConnectionsRDDTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ConnectionsRDDTestCase") \
            .master("local[*]") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .getOrCreate()
        df = cls.spark.read.format("org.elasticsearch.spark.sql").load("2_connections_data_300k")
        cls.rdd = df.rdd

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rdd_count(self):
        count = self.rdd.count()
        self.assertGreater(count, 1000, "Connections RDD should have more than 1000 records")

    def test_rdd_relationship_filter(self):
        friends_count = self.rdd.filter(lambda row: row.relationship == "friend").count()
        self.assertGreater(friends_count, 0, "Should have connections with relationship 'friend'")

if __name__ == "__main__":
    unittest.main()
