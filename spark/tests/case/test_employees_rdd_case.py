import unittest
from pyspark.sql import SparkSession

class EmployeesRDDTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("EmployeesRDDTestCase") \
            .master("local[*]") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .getOrCreate()
        # Đọc DataFrame sau đó chuyển sang RDD
        df = cls.spark.read.format("org.elasticsearch.spark.sql").load("employees")
        cls.rdd = df.rdd

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rdd_count(self):
        count = self.rdd.count()
        self.assertGreater(count, 0, "RDD should have more than 0 records")

    def test_rdd_salary_filter(self):
        salary_over_60000 = self.rdd.filter(lambda row: row.salary is not None and row.salary > 60000).count()
        self.assertGreater(salary_over_60000, 0, "Should have employees with salary over 60000")

if __name__ == "__main__":
    unittest.main()
