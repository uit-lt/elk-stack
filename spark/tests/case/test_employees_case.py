import unittest
from pyspark.sql import SparkSession

class EmployeesTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("EmployeesTestCase") \
            .config("spark.es.nodes", "elasticsearch") \
            .config("spark.es.port", "9200") \
            .getOrCreate()
        cls.df = cls.spark.read.format("org.elasticsearch.spark.sql").load("employees")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_count_employees(self):
        count = self.df.count()
        self.assertGreater(count, 0, "Employees index should not be empty")

    def test_salary_stats(self):
        stats = self.df.selectExpr("min(salary) as min_salary", "max(salary) as max_salary").collect()[0]
        self.assertIsNotNone(stats.min_salary, "Min salary should not be None")
        self.assertIsNotNone(stats.max_salary, "Max salary should not be None")
        self.assertLess(stats.min_salary, stats.max_salary, "Min salary should be less than max salary")

if __name__ == "__main__":
    unittest.main()
