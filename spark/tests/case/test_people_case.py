import unittest
from pyspark.sql import SparkSession

class PeopleTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PeopleTestCase") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .getOrCreate()
        cls.df = cls.spark.read.format("org.elasticsearch.spark.sql").load("2_people_data_2k")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_count_people(self):
        count = self.df.count()
        self.assertGreater(count, 0, "People index should not be empty")

    def test_gender_distribution(self):
        age_counts = self.df.groupBy("age").count().collect()
        self.assertTrue(len(age_counts) > 0, "Should have at least one age category")

if __name__ == "__main__":
    unittest.main()
