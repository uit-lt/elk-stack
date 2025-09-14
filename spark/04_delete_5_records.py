from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import time
import urllib.request
import urllib.error
import pandas as pd
from IPython.display import display, HTML, Markdown
import matplotlib.pyplot as plt
import seaborn as sns

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("Delete5Records") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    display(Markdown("## 🗑️ 4. DELETE 5 RECORDS"))

    # Create Spark Session
    spark = create_spark_session()
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")

    # Define the IDs of records to delete (same as updated in step 3)
    delete_ids = [24410114, 24410100, 24410109, 24410092, 24410040]

    display(Markdown("### 🎯 Records to Delete"))
    display(HTML(f"<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'><b>🎯 Target IDs: {', '.join(map(str, delete_ids))}</b></div>"))

    try:
        # Read current state before deletion
        display(Markdown("### 📋 Current State Before Deletion"))
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
                display(HTML(f"<div style='background-color: #e8f5e8; padding: 10px; border-radius: 5px;'><b>📊 Found {current_count} records to delete:</b></div>"))
                current_records = current_df.select("id", "name", "age").orderBy("id").toPandas()
                styled_current = current_records.style.background_gradient(cmap='Reds', subset=['age']) \
                                                      .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                                      .set_caption("Records to be Deleted")
                display(styled_current)
            else:
                display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⚠️ No records found to delete - may need to run insert/update steps first</div>"))

        except Exception as read_error:
            display(HTML(f"<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'>⚠️ Could not read current records: {str(read_error)}</div>"))

        # Get total count before deletion
        display(Markdown("### 📈 Total Count Before Deletion"))
        try:
            all_records_before = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .load()

            total_before = all_records_before.count()
            display(HTML(f"<div style='background-color: #cce5ff; padding: 10px; border-radius: 5px;'><b>📊 Total records in index before deletion: {total_before:,}</b></div>"))

        except Exception as count_error:
            display(HTML(f"<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'>⚠️ Could not get total count: {str(count_error)}</div>"))
            total_before = "unknown"

        # Delete records using urllib (since Spark doesn't have native delete operation)
        display(Markdown("### 🗑️ Deleting Records via HTTP Requests"))
        display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⏳ Deleting records via Elasticsearch REST API...</div>"))

        deleted_count = 0
        deletion_results = []

        for record_id in delete_ids:
            try:
                # Use Python urllib to delete via Elasticsearch REST API
                url = f"http://elasticsearch:9200/2_people_data_2k/_doc/{record_id}"
                req = urllib.request.Request(url, method='DELETE')

                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        if response.status in [200, 404]:  # 200 = deleted, 404 = not found (already deleted)
                            deletion_results.append((record_id, "✅ Success", f"Status: {response.status}"))
                            deleted_count += 1
                        else:
                            deletion_results.append((record_id, "❌ Failed", f"Status: {response.status}"))
                except urllib.error.HTTPError as e:
                    if e.code == 404:  # Not found (already deleted)
                        deletion_results.append((record_id, "✅ Success", "Already deleted or not found"))
                        deleted_count += 1
                    else:
                        deletion_results.append((record_id, "❌ Failed", f"HTTP Error: {e.code}"))

            except Exception as delete_error:
                deletion_results.append((record_id, "❌ Error", str(delete_error)))

        # Display deletion results in a table
        results_df = pd.DataFrame(deletion_results, columns=['ID', 'Status', 'Details'])
        styled_results = results_df.style.applymap(lambda x: 'background-color: #d4edda' if '✅' in x else 'background-color: #f8d7da' if '❌' in x else '', subset=['Status']) \
                                          .set_caption("Deletion Results")
        display(styled_results)

        display(HTML(f"<div style='background-color: #d1ecf1; padding: 10px; border-radius: 5px;'>📊 Successfully processed {deleted_count} deletion requests</div>"))

        # Wait for deletion to complete
        display(HTML("<div style='background-color: #e2e3e5; padding: 10px; border-radius: 5px;'>⏱️ Waiting 3 seconds for deletion to complete...</div>"))
        time.sleep(3)

        # Verify deletions
        display(Markdown("### 🔍 Verification of Deletions"))
        try:
            verification_df = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
                .load()

            remaining_count = verification_df.count()

            if remaining_count == 0:
                display(HTML("<div style='background-color: #d4edda; padding: 10px; border-radius: 5px;'>✅ All target records successfully deleted!</div>"))
            else:
                display(HTML(f"<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⚠️ {remaining_count} records still exist:</div>"))
                remaining_records = verification_df.select("id", "name", "age").orderBy("id").toPandas()
                styled_remaining = remaining_records.style.background_gradient(cmap='Yellows', subset=['age']) \
                                                          .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                                          .set_caption("Records Still Remaining")
                display(styled_remaining)

            # Get total count after deletion
            all_records_after = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .load()

            total_after = all_records_after.count()
            display(HTML(f"<div style='background-color: #cce5ff; padding: 10px; border-radius: 5px;'><b>📊 Total records in index after deletion: {total_after:,}</b></div>"))

            if isinstance(total_before, int):
                actual_deleted = total_before - total_after
                display(HTML(f"<div style='background-color: #e8f5e8; padding: 10px; border-radius: 5px;'><b>🗑️ Actually deleted records: {actual_deleted}</b></div>"))

                # Create a simple before/after comparison chart
                if actual_deleted > 0:
                    fig, ax = plt.subplots(1, 1, figsize=(8, 6))

                    categories = ['Before Deletion', 'After Deletion', 'Deleted']
                    values = [total_before, total_after, actual_deleted]
                    colors = ['lightblue', 'lightgreen', 'lightcoral']

                    bars = ax.bar(categories, values, color=colors, alpha=0.8)
                    ax.set_ylabel('Record Count')
                    ax.set_title('Deletion Impact Summary')

                    # Add value labels on bars
                    for bar in bars:
                        height = bar.get_height()
                        ax.text(bar.get_x() + bar.get_width()/2., height + max(values) * 0.01,
                               f'{int(height):,}', ha='center', va='bottom')

                    plt.tight_layout()
                    plt.show()

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
            <h4 style='margin: 0;'>✅ DELETE 5 RECORDS - COMPLETED SUCCESSFULLY!</h4>
            <p style='margin: 5px 0 0 0;'>Deletion operation completed.</p>
            <ul style='margin: 10px 0 0 20px;'>
                <li>Attempted to delete: {len(delete_ids)} records</li>
                <li>Successfully processed: {deleted_count} records</li>
                <li>Target IDs: {', '.join(map(str, delete_ids))}</li>
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
