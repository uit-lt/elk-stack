# SPARK 5 SEPARATE OPERATIONS - ELASTICSEARCH CRUD

## Overview
This document provides commands to execute 5 separate Spark operations with Elasticsearch:
1. **GET ALL DATA** - Read and display all records from `2_people_data_2k` index
2. **INSERT 5 RECORDS** - Insert 5 new records with custom IDs
3. **UPDATE 5 RECORDS** - Update the previously inserted records
4. **DELETE 5 RECORDS** - Delete the updated records
5. **QUERY BY IDs** - Query specific records by their IDs

## Prerequisites
- ✅ Docker containers running (spark-master, spark-worker, elasticsearch, kibana)
- ✅ Elasticsearch index `2_people_data_2k` populated with data
- ✅ All Python scripts located in `/opt/spark/work-dir/` in spark-master container

## Commands

### Step 1: GET ALL DATA
**Description**: Reads all 2001 records from Elasticsearch and displays them with age distribution statistics.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 \
/opt/spark/work-dir/01_get_all_data.py
```
**Expected Result**: Shows all 2001 records + age distribution across 64 different ages

### Step 2: INSERT 5 RECORDS
**Description**: Inserts 5 new records with IDs [24410114, 24410100, 24410109, 24410092, 24410040].

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 \
/opt/spark/work-dir/02_insert_5_records.py
```
**Expected Result**: 5 new records inserted, total records: 2001 → 2006

### Step 3: UPDATE 5 RECORDS
**Description**: Updates the 5 previously inserted records by adding "updated" suffix to names.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 \
/opt/spark/work-dir/03_update_5_records.py
```
**Expected Result**: 5 records updated with "updated" suffix in names

### Step 4: DELETE 5 RECORDS
**Description**: Deletes the 5 updated records using HTTP DELETE requests.

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 \
/opt/spark/work-dir/04_delete_5_records.py
```
**Expected Result**: 5 records deleted, total records: 2006 → 2001

### Step 5: QUERY BY IDs
**Description**: Query specific records by their IDs (configurable list in script).

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 \
/opt/spark/work-dir/05_query_by_ids.py
```
**Expected Result**: Shows matching records or "No records found" if IDs don't exist

## Notes
- **Package Dependency**: `--packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3` is required for all operations
- **ID Configuration**: Edit `query_ids` list in `05_query_by_ids.py` to search different IDs
- **Data Schema**: All operations work with schema: `{id: int, name: string, age: int}`
- **Error Handling**: All scripts include verification and error handling
- **Index**: All operations target the `2_people_data_2k` Elasticsearch index

## Troubleshooting
If you encounter permission errors:
```bash
docker exec --user root spark-master mkdir -p /home/spark/.ivy2/cache
docker exec --user root spark-master chown -R spark:spark /home/spark
```

## Manual Verification Commands
Check Elasticsearch directly:
```bash
# Get total count
curl -X GET "localhost:9200/2_people_data_2k/_count"

# Search for specific IDs
curl -X GET "localhost:9200/2_people_data_2k/_search" -H 'Content-Type: application/json' -d'
{"query":{"terms":{"id":[1,2,3,4,5]}},"size":10}'
```

---
**Status**: ✅ All operations tested and working
**Last Updated**: 2025-09-14


📋 HOW TO USE - SPARK ELK STACK OPERATIONS

🚀 Quick Start Commands

Run via Spark Submit (Command Line):
# 1. Get all data
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 /opt/spark/work-dir/01_get_all_data.py

# 2. Insert 5 records
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 /opt/spark/work-dir/02_insert_5_records.py

# 3. Update 5 records
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 /opt/spark/work-dir/03_update_5_records.py

# 4. Delete 5 records
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 /opt/spark/work-dir/04_delete_5_records.py

# 5. Query by IDs
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3 /opt/spark/work-dir/05_query_by_ids.py
