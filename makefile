CONTAINER ?= spark-master
TEST_FILE ?= test_duplicate_people_names.py
SPARK_PACKAGES ?= --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3

run:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/$(TEST_FILE)"

# Spark ELK Stack Operations - Shortened Commands
spark-get-all:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/crud/01_get_all_data.py"

spark-insert:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/crud/02_insert_5_records.py"

spark-update:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/crud/03_update_5_records.py"

spark-delete:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/crud/04_delete_5_records.py"

spark-query:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/crud/05_query_by_ids.py"

spark-to-elastic:
	docker exec $(CONTAINER) spark-submit \
		--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.12.2 \
		/opt/bitnami/spark/tests/csv_to_elasticsearch.py
