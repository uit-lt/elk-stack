#!/usr/bin/env python3
"""
Script to create Jupyter notebooks for Spark Elasticsearch operations
This script creates ready-to-use notebooks without requiring file conversion
"""
import json
import os
from pathlib import Path

def create_notebook_01():
    """Create notebook for Get All Data"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# 📊 Get All Data from Elasticsearch"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import *\n",
                    "import pandas as pd"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark Session\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"GetAllData\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.es.nodes\", \"elasticsearch\") \\\n",
                    "    .config(\"spark.es.port\", \"9200\") \\\n",
                    "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                    "    .config(\"spark.jars.packages\", \"org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(f\"Application ID: {spark.sparkContext.applicationId}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Read all data from people index\n",
                    "print(\"Reading from 2_people_data_2k index...\")\n",
                    "people_df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()\n",
                    "\n",
                    "# Show total count\n",
                    "total_count = people_df.count()\n",
                    "print(f\"Total records: {total_count}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Show sample data\n",
                    "print(\"Sample data (first 10 records):\")\n",
                    "people_df.show(10)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Show age distribution\n",
                    "print(\"Age distribution:\")\n",
                    "age_dist = people_df.groupBy(\"age\").count().orderBy(\"age\")\n",
                    "age_dist.show(20)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Show name statistics\n",
                    "print(\"Most common names (top 10):\")\n",
                    "name_stats = people_df.groupBy(\"name\").count().orderBy(col(\"count\").desc())\n",
                    "name_stats.show(10)"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session\n",
                    "spark.stop()\n",
                    "print(\"Spark session stopped.\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def create_notebook_02():
    """Create notebook for Insert 5 Records"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# 📝 Insert 5 Records"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import *\n",
                    "from pyspark.sql.types import StructType, StructField, IntegerType, StringType"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark Session\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"Insert5Records\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.es.nodes\", \"elasticsearch\") \\\n",
                    "    .config(\"spark.es.port\", \"9200\") \\\n",
                    "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                    "    .config(\"spark.jars.packages\", \"org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(f\"Application ID: {spark.sparkContext.applicationId}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Define schema for new records\n",
                    "schema = StructType([\n",
                    "    StructField(\"id\", IntegerType(), True),\n",
                    "    StructField(\"name\", StringType(), True),\n",
                    "    StructField(\"age\", IntegerType(), True)\n",
                    "])\n",
                    "\n",
                    "# Create 5 new records\n",
                    "new_data = [\n",
                    "    (24410114, \"Tran Tireu Thuan\", 30),\n",
                    "    (24410100, \"Nguyen Phuong Tan\", 30),\n",
                    "    (24410109, \"Nguyen Thi Thu Thao\", 28),\n",
                    "    (24410092, \"Huynh Duy Quoc\", 35),\n",
                    "    (24410040, \"Ha Huy Hung\", 22)\n",
                    "]\n",
                    "\n",
                    "print(\"Creating 5 new records:\")\n",
                    "for record in new_data:\n",
                    "    print(f\"  ID: {record[0]}, Name: {record[1]}, Age: {record[2]}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create DataFrame\n",
                    "new_df = spark.createDataFrame(new_data, schema)\n",
                    "new_df.show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Insert to Elasticsearch\n",
                    "print(\"Inserting records to Elasticsearch...\")\n",
                    "try:\n",
                    "    new_df.write \\\n",
                    "        .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "        .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "        .option(\"es.port\", \"9200\") \\\n",
                    "        .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "        .option(\"es.mapping.id\", \"id\") \\\n",
                    "        .mode(\"append\") \\\n",
                    "        .save()\n",
                    "    print(\"✓ Records inserted successfully!\")\n",
                    "except Exception as e:\n",
                    "    print(f\"✗ Insert failed: {e}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Verify insertion\n",
                    "print(\"Verifying insertion...\")\n",
                    "all_df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()\n",
                    "\n",
                    "new_total = all_df.count()\n",
                    "print(f\"Total records after insertion: {new_total}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session\n",
                    "spark.stop()\n",
                    "print(\"Spark session stopped.\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def create_notebook_03():
    """Create notebook for Update 5 Records"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# 🔄 Update 5 Records"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import *"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark Session\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"Update5Records\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.es.nodes\", \"elasticsearch\") \\\n",
                    "    .config(\"spark.es.port\", \"9200\") \\\n",
                    "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                    "    .config(\"spark.jars.packages\", \"org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(f\"Application ID: {spark.sparkContext.applicationId}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Define updated records data\n",
                    "updated_records_data = [\n",
                    "    (24410114, \"Tran Tireu Thuan updated\", 30),\n",
                    "    (24410100, \"Nguyen Phuong Tan updated\", 30),\n",
                    "    (24410109, \"Nguyen Thi Thu Thao updated\", 28),\n",
                    "    (24410092, \"Huynh Duy Quoc updated\", 35),\n",
                    "    (24410040, \"Ha Huy Hung updated\", 22)\n",
                    "]\n",
                    "\n",
                    "update_ids = [record[0] for record in updated_records_data]\n",
                    "print(f\"Updating records with IDs: {update_ids}\")\n",
                    "\n",
                    "# Read current data\n",
                    "current_df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Show records before update\n",
                    "records_before_update = current_df.filter(col(\"id\").isin(update_ids))\n",
                    "print(\"Records before update:\")\n",
                    "records_before_update.show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create DataFrame with updated data\n",
                    "from pyspark.sql.types import StructType, StructField, IntegerType, StringType\n",
                    "\n",
                    "schema = StructType([\n",
                    "    StructField(\"id\", IntegerType(), True),\n",
                    "    StructField(\"name\", StringType(), True),\n",
                    "    StructField(\"age\", IntegerType(), True)\n",
                    "])\n",
                    "\n",
                    "updated_df = spark.createDataFrame(updated_records_data, schema)\n",
                    "\n",
                    "print(\"Records after update:\")\n",
                    "updated_df.show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Write updated records back to Elasticsearch\n",
                    "print(\"Writing updated records to Elasticsearch...\")\n",
                    "try:\n",
                    "    updated_df.write \\\n",
                    "        .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "        .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "        .option(\"es.port\", \"9200\") \\\n",
                    "        .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "        .option(\"es.mapping.id\", \"id\") \\\n",
                    "        .mode(\"append\") \\\n",
                    "        .save()\n",
                    "    print(\"✓ Records updated successfully!\")\n",
                    "except Exception as e:\n",
                    "    print(f\"✗ Update failed: {e}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session\n",
                    "spark.stop()\n",
                    "print(\"Spark session stopped.\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def create_notebook_04():
    """Create notebook for Delete 5 Records"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# 🗑️ Delete 5 Records"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import *\n",
                    "import requests"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark Session\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"Delete5Records\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.es.nodes\", \"elasticsearch\") \\\n",
                    "    .config(\"spark.es.port\", \"9200\") \\\n",
                    "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                    "    .config(\"spark.jars.packages\", \"org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(f\"Application ID: {spark.sparkContext.applicationId}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "def delete_record_by_id(doc_id):\n",
                    "    \"\"\"Delete a record from Elasticsearch by ID using HTTP DELETE\"\"\"\n",
                    "    url = f\"http://elasticsearch:9200/2_people_data_2k/_doc/{doc_id}\"\n",
                    "    try:\n",
                    "        response = requests.delete(url)\n",
                    "        return response.status_code == 200\n",
                    "    except Exception as e:\n",
                    "        print(f\"Error deleting record {doc_id}: {e}\")\n",
                    "        return False"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# IDs to delete\n",
                    "delete_ids = [24410114, 24410100, 24410109, 24410092, 24410040]\n",
                    "print(f\"Deleting records with IDs: {delete_ids}\")\n",
                    "\n",
                    "# Read current data to show records before deletion\n",
                    "current_df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()\n",
                    "\n",
                    "# Show records before deletion\n",
                    "records_to_delete = current_df.filter(col(\"id\").isin(delete_ids))\n",
                    "print(f\"Records to delete ({records_to_delete.count()}):\")\n",
                    "records_to_delete.show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Delete records using HTTP DELETE\n",
                    "print(\"Deleting records...\")\n",
                    "deleted_count = 0\n",
                    "for doc_id in delete_ids:\n",
                    "    if delete_record_by_id(doc_id):\n",
                    "        print(f\"✓ Deleted record ID: {doc_id}\")\n",
                    "        deleted_count += 1\n",
                    "    else:\n",
                    "        print(f\"✗ Failed to delete record ID: {doc_id}\")\n",
                    "\n",
                    "print(f\"Deleted {deleted_count} out of {len(delete_ids)} records\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Verify deletion\n",
                    "print(\"Verifying deletion...\")\n",
                    "updated_df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()\n",
                    "\n",
                    "remaining_records = updated_df.filter(col(\"id\").isin(delete_ids))\n",
                    "remaining_count = remaining_records.count()\n",
                    "total_count = updated_df.count()\n",
                    "\n",
                    "print(f\"Remaining records with deleted IDs: {remaining_count}\")\n",
                    "print(f\"Total records after deletion: {total_count}\")\n",
                    "\n",
                    "if remaining_count > 0:\n",
                    "    print(\"Remaining records:\")\n",
                    "    remaining_records.show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session\n",
                    "spark.stop()\n",
                    "print(\"Spark session stopped.\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def create_notebook_05():
    """Create notebook for Query by IDs"""
    return {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["# 🔍 Query Records by IDs"]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "from pyspark.sql import SparkSession\n",
                    "from pyspark.sql.functions import *"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Create Spark Session\n",
                    "spark = SparkSession.builder \\\n",
                    "    .appName(\"QueryByIds\") \\\n",
                    "    .master(\"local[*]\") \\\n",
                    "    .config(\"spark.es.nodes\", \"elasticsearch\") \\\n",
                    "    .config(\"spark.es.port\", \"9200\") \\\n",
                    "    .config(\"spark.serializer\", \"org.apache.spark.serializer.KryoSerializer\") \\\n",
                    "    .config(\"spark.jars.packages\", \"org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3\") \\\n",
                    "    .getOrCreate()\n",
                    "\n",
                    "print(f\"Application ID: {spark.sparkContext.applicationId}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# List of IDs to query (you can modify this list)\n",
                    "query_ids = [1, 2, 3, 4, 5, 100, 200, 500, 1000, 1500]\n",
                    "print(f\"Searching for IDs: {query_ids}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Read data from Elasticsearch\n",
                    "df = spark.read \\\n",
                    "    .format(\"org.elasticsearch.spark.sql\") \\\n",
                    "    .option(\"es.nodes\", \"elasticsearch\") \\\n",
                    "    .option(\"es.port\", \"9200\") \\\n",
                    "    .option(\"es.resource\", \"2_people_data_2k\") \\\n",
                    "    .load()\n",
                    "\n",
                    "print(f\"Total records in index: {df.count()}\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Filter by IDs\n",
                    "filtered_df = df.filter(col(\"id\").isin(query_ids))\n",
                    "found_count = filtered_df.count()\n",
                    "\n",
                    "print(f\"Found {found_count} matching records:\")\n",
                    "\n",
                    "if found_count > 0:\n",
                    "    # Show all matching records\n",
                    "    filtered_df.orderBy(\"id\").show()"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "if found_count > 0:\n",
                    "    # Show summary statistics\n",
                    "    print(\"Summary statistics for found records:\")\n",
                    "    age_stats = filtered_df.agg(min('age'), max('age'), avg('age')).collect()[0]\n",
                    "    print(f\"  - Age range: {age_stats[0]} to {age_stats[1]}\")\n",
                    "    print(f\"  - Average age: {age_stats[2]:.1f}\")\n",
                    "    \n",
                    "    # Show which IDs were found vs not found\n",
                    "    found_ids = [row['id'] for row in filtered_df.select('id').collect()]\n",
                    "    not_found_ids = [id for id in query_ids if id not in found_ids]\n",
                    "    \n",
                    "    print(f\"Found IDs: {sorted(found_ids)}\")\n",
                    "    if not_found_ids:\n",
                    "        print(f\"Not found IDs: {sorted(not_found_ids)}\")\n",
                    "else:\n",
                    "    print(\"No records found with the specified IDs.\")"
                ]
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# Stop Spark session\n",
                    "spark.stop()\n",
                    "print(\"Spark session stopped.\")"
                ]
            }
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3 (ipykernel)",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python",
                "version": "3.8.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }

def main():
    """Create all Jupyter notebooks"""
    notebooks = [
        ("01_Get_All_Data.ipynb", create_notebook_01()),
        ("02_Insert_5_Records.ipynb", create_notebook_02()),
        ("03_Update_5_Records.ipynb", create_notebook_03()),
        ("04_Delete_5_Records.ipynb", create_notebook_04()),
        ("05_Query_By_IDs.ipynb", create_notebook_05())
    ]

    print("Creating Spark Elasticsearch notebooks...")
    print("=" * 50)

    current_dir = Path(__file__).parent

    for filename, notebook_content in notebooks:
        notebook_path = current_dir / filename

        with open(notebook_path, 'w', encoding='utf-8') as f:
            json.dump(notebook_content, f, indent=2, ensure_ascii=False)

        print(f"✓ Created: {filename}")

    print("=" * 50)
    print("🎉 All notebooks created successfully!")
    print("\nYou can now run these notebooks in Jupyter Lab:")
    for filename, _ in notebooks:
        print(f"  - {filename}")

if __name__ == "__main__":
    main()
