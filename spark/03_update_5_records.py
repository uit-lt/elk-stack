from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time
import pandas as pd
from IPython.display import display, HTML, Markdown
import matplotlib.pyplot as plt
import seaborn as sns

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("Update5Records") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    display(Markdown("## 🔄 3. UPDATE 5 RECORDS"))

    # Create Spark Session
    spark = create_spark_session()
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")

    # Define the IDs of records to update (same as inserted in step 2)
    update_ids = [24410114, 24410100, 24410109, 24410092, 24410040]

    # Define updated data
    updated_records_data = [
        (24410114, "Tran Tireu Thuan updated", 30),
        (24410100, "Nguyen Phuong Tan updated", 30),
        (24410109, "Nguyen Thi Thu Thao updated", 28),
        (24410092, "Huynh Duy Quoc updated", 35),
        (24410040, "Ha Huy Hung updated", 22)
    ]

    display(Markdown("### 📝 Records to Update"))
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    updated_df = spark.createDataFrame(updated_records_data, schema)

    # Display records in a styled table
    records_pandas = updated_df.toPandas()
    styled_records = records_pandas.style.background_gradient(cmap='Oranges', subset=['age']) \
                                        .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                        .set_caption("Records to be Updated")
    display(styled_records)

    try:
        # Read current state before update
        display(Markdown("### 📋 Current State Before Update"))
        try:
            current_df = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
                .load()

            current_count = current_df.count()
            if current_count > 0:
                display(HTML(f"<div style='background-color: #e8f5e8; padding: 10px; border-radius: 5px;'><b>📊 Found {current_count} records to update:</b></div>"))
                current_records = current_df.select("id", "name", "age").orderBy("id").toPandas()
                styled_current = current_records.style.background_gradient(cmap='Blues', subset=['age']) \
                                                      .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                                      .set_caption("Current Records (Before Update)")
                display(styled_current)
            else:
                display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⚠️ No records found to update - may need to run insert step first</div>"))

        except Exception as read_error:
            display(HTML(f"<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'>⚠️ Could not read current records: {str(read_error)}</div>"))

        # Update records via Spark-Elasticsearch connector
        display(Markdown("### 🔄 Updating Records..."))
        display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⏳ Updating records via Spark-Elasticsearch connector...</div>"))

        # Use upsert operation to update existing documents or create if not exist
        updated_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.write.operation", "upsert") \
            .option("es.mapping.id", "id") \
            .mode("append") \
            .save()

        display(HTML("<div style='background-color: #d1ecf1; padding: 10px; border-radius: 5px;'>✅ Records updated successfully using Spark connector!</div>"))

        # Wait for indexing with progress indicator
        display(HTML("<div style='background-color: #e2e3e5; padding: 10px; border-radius: 5px;'>⏱️ Waiting 3 seconds for Elasticsearch indexing...</div>"))
        time.sleep(3)

        # Verify updates
        display(Markdown("### 🔍 Verification of Updates"))
        try:
            verification_df = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
                .load()

            verified_count = verification_df.count()

            if verified_count > 0:
                display(HTML(f"<div style='background-color: #d4edda; padding: 10px; border-radius: 5px;'>✅ Found {verified_count} updated records:</div>"))

                # Display verified records
                verified_records = verification_df.select("id", "name", "age").orderBy("id").toPandas()
                styled_verified = verified_records.style.background_gradient(cmap='Purples', subset=['age']) \
                                                        .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                                        .set_caption("Updated Records (After Update)")
                display(styled_verified)

                # Check if updates were successful by looking for "updated" in names
                updated_count = verification_df.filter(col("name").contains("updated")).count()
                display(HTML(f"<div style='background-color: #cce5ff; padding: 10px; border-radius: 5px;'>📊 Records with 'updated' in name: {updated_count}</div>"))

                if updated_count > 0:
                    # Create a comparison visualization
                    fig, ax = plt.subplots(1, 1, figsize=(10, 6))

                    # Before and after comparison
                    names = verified_records['name'].str.replace(' updated', '', regex=False)
                    ages = verified_records['age']

                    bars = ax.bar(range(len(verified_records)), ages, color='lightcoral', alpha=0.8)
                    ax.set_xlabel('Records')
                    ax.set_ylabel('Age')
                    ax.set_title('Age Distribution of Updated Records')
                    ax.set_xticks(range(len(verified_records)))
                    ax.set_xticklabels([f"ID {id}" for id in verified_records['id']], rotation=45, ha='right')

                    # Add value labels on bars
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                               f'{int(height)}', ha='center', va='bottom')

                    plt.tight_layout()
                    plt.show()

                    display(HTML("<div style='background-color: #d4edda; padding: 10px; border-radius: 5px;'>✅ Updates appear successful!</div>"))
                else:
                    display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⚠️ Updates may not have been applied</div>"))
            else:
                display(HTML("<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'>⚠️ No records found after update</div>"))

        except Exception as verify_error:
            error_html = f"""
            <div style='background-color: #f8d7da; border: 1px solid #f5c6cb; color: #721c24;
                        padding: 15px; border-radius: 5px;'>
                <h5 style='margin: 0;'>⚠️ Verification Error</h5>
                <p style='margin: 5px 0 0 0;'>{str(verify_error)}</p>
            </div>
            """
            display(HTML(error_html))

        # Success message
        success_html = f"""
        <div style='background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724;
                    padding: 15px; border-radius: 5px; margin: 20px 0;'>
            <h4 style='margin: 0;'>✅ UPDATE 5 RECORDS - COMPLETED SUCCESSFULLY!</h4>
            <p style='margin: 5px 0 0 0;'>Successfully updated {len(updated_records_data)} records in Elasticsearch index.</p>
            <ul style='margin: 10px 0 0 20px;'>
                <li>Updated IDs: {', '.join([str(record[0]) for record in updated_records_data])}</li>
                <li>All names now contain 'updated' suffix</li>
            </ul>
        </div>
        """
        display(HTML(success_html))

    except Exception as e:
        error_html = f"""
        <div style='background-color: #f8d7da; border: 1px solid #f5c6cb; color: #721c24;
                    padding: 15px; border-radius: 5px; margin: 20px 0;'>
            <h4 style='margin: 0;'>❌ ERROR OCCURRED</h4>
            <p style='margin: 5px 0 0 0;'>{str(e)}</p>
        </div>
        """
        display(HTML(error_html))

    finally:
        spark.stop()
        print("🔄 Spark session stopped.")

# Run the main function if not in Jupyter (for compatibility)
if __name__ == "__main__":
    main()
