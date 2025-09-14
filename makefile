CONTAINER ?= spark-master
TEST_FILE ?= test_duplicate_people_names.py

run:
	docker exec -it $(CONTAINER) bash -c "spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/tests/$(TEST_FILE)"
