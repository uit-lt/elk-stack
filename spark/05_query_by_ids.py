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
        .appName("QueryByIds") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def main():
    display(Markdown("## 🔍 5. QUERY RECORDS BY ID LIST"))

    # Create Spark Session
    spark = create_spark_session()
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")

    try:
        # --- CONFIGURATION SECTION ---
        # TODO: Update this list with your desired IDs
        query_ids = [24410114, 24410100, 24410109, 24410092, 24410040] # Example IDs - CHANGE THESE

        display(Markdown("### 🎯 Query Configuration"))
        display(HTML(f"<div style='background-color: #e8f5e8; padding: 15px; border-radius: 5px;'><b>🔍 Querying for IDs:</b> {', '.join(map(str, query_ids))}</div>"))
        display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px; margin-top: 10px;'>⚠️ <strong>Note:</strong> Update the query_ids list in the code to search for different IDs</div>"))

        # Read data from Elasticsearch
        display(Markdown("### 📚 Reading from Elasticsearch Index"))
        people_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .load()

        total_count = people_df.count()
        display(HTML(f"<div style='background-color: #cce5ff; padding: 10px; border-radius: 5px;'><b>📊 Total records in index: {total_count:,}</b></div>"))

        # Filter by ID list
        display(Markdown("### 🔎 Filtering by ID List"))
        display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⏳ Filtering records by ID list...</div>"))

        filtered_df = people_df.filter(people_df.id.isin(query_ids))

        found_count = filtered_df.count()
        display(HTML(f"<div style='background-color: #d1ecf1; padding: 10px; border-radius: 5px;'><b>🎯 Found {found_count} records matching the ID list</b></div>"))

        if found_count > 0:
            display(Markdown("### 📊 Matching Records"))
            # Collect and display results in a styled pandas DataFrame
            results = filtered_df.select("id", "name", "age").orderBy("id").toPandas()

            styled_results = results.style.background_gradient(cmap='Greens', subset=['age']) \
                                          .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                          .set_caption("Query Results")
            display(styled_results)

            # Show statistics in a nice format
            display(Markdown("### 📈 Statistics for Found Records"))
            stats_df = filtered_df.agg(
                min("age").alias("min_age"),
                max("age").alias("max_age"),
                avg("age").alias("avg_age"),
                count("*").alias("found_records")
            ).collect()[0]

            # Create a nice stats display
            stats_html = f"""
            <div style='display: flex; gap: 15px; margin: 20px 0;'>
                <div style='background-color: #f0f8ff; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                    <h4 style='color: #2c3e50; margin: 0;'>Min Age</h4>
                    <p style='font-size: 20px; font-weight: bold; color: #e74c3c; margin: 5px 0;'>{stats_df['min_age']}</p>
                </div>
                <div style='background-color: #f0fff0; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                    <h4 style='color: #2c3e50; margin: 0;'>Max Age</h4>
                    <p style='font-size: 20px; font-weight: bold; color: #27ae60; margin: 5px 0;'>{stats_df['max_age']}</p>
                </div>
                <div style='background-color: #fff8f0; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                    <h4 style='color: #2c3e50; margin: 0;'>Avg Age</h4>
                    <p style='font-size: 20px; font-weight: bold; color: #f39c12; margin: 5px 0;'>{stats_df['avg_age']:.1f}</p>
                </div>
                <div style='background-color: #f8f0ff; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                    <h4 style='color: #2c3e50; margin: 0;'>Found</h4>
                    <p style='font-size: 20px; font-weight: bold; color: #8e44ad; margin: 5px 0;'>{stats_df['found_records']}</p>
                </div>
            </div>
            """
            display(HTML(stats_html))

            # Create visualization of found records
            if len(results) > 1:
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

                # Age distribution of found records
                ax1.bar(range(len(results)), results['age'], color='skyblue', alpha=0.7)
                ax1.set_xlabel('Record Index')
                ax1.set_ylabel('Age')
                ax1.set_title('Age Distribution of Found Records')
                ax1.set_xticks(range(len(results)))
                ax1.set_xticklabels([f"ID {id}" for id in results['id']], rotation=45, ha='right')

                # Pie chart showing found vs requested
                found_ids = results['id'].tolist()
                missing_count = len(query_ids) - len(found_ids)
                if missing_count > 0:
                    pie_data = [len(found_ids), missing_count]
                    pie_labels = ['Found', 'Missing']
                    colors = ['lightgreen', 'lightcoral']
                    ax2.pie(pie_data, labels=pie_labels, colors=colors, autopct='%1.1f%%', startangle=90)
                    ax2.set_title('Query Results Summary')
                else:
                    ax2.pie([len(found_ids)], labels=['All Found'], colors=['lightgreen'], autopct='%1.1f%%', startangle=90)
                    ax2.set_title('Query Results Summary')

                plt.tight_layout()
                plt.show()

            # Show which IDs were not found
            display(Markdown("### 🔍 Query Analysis"))
            found_ids = results['id'].tolist()
            missing_ids = [id for id in query_ids if id not in found_ids]

            if missing_ids:
                display(HTML(f"<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⚠️ <strong>IDs not found in index:</strong> {', '.join(map(str, missing_ids))}</div>"))
            else:
                display(HTML("<div style='background-color: #d4edda; padding: 10px; border-radius: 5px;'>✅ <strong>All requested IDs were found!</strong></div>"))

        else:
            display(HTML(f"<div style='background-color: #f8d7da; padding: 15px; border-radius: 5px;'><h5 style='margin: 0;'>❌ No Records Found</h5><p style='margin: 5px 0 0 0;'>No records found for IDs: {', '.join(map(str, query_ids))}</p><p style='margin: 5px 0 0 0;'>Please check if these IDs exist in the index.</p></div>"))

        # Success message
        success_html = f"""
        <div style='background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724;
                    padding: 15px; border-radius: 5px; margin: 20px 0;'>
            <h4 style='margin: 0;'>✅ QUERY BY IDs - COMPLETED SUCCESSFULLY!</h4>
            <p style='margin: 5px 0 0 0;'>Query operation completed successfully.</p>
            <ul style='margin: 10px 0 0 20px;'>
                <li>Searched for {len(query_ids)} IDs: {', '.join(map(str, query_ids))}</li>
                <li>Found {found_count} matching records</li>
                <li>Total records in index: {total_count:,}</li>
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
