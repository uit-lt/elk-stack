```bash
docker exec -it spark-master bash spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/test_spark.py

docker exec -it spark-worker bash spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/test_spark.py

docker exec -it spark-worker-2 bash spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/test_spark.py
```
