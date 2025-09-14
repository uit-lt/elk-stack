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
        .appName("Insert5Records") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    display(Markdown("## 📝 2. INSERT 5 RECORDS"))

    # Create Spark Session
    spark = create_spark_session()
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")

    # Define schema for new records
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    # Create 5 new records with specific student IDs
    new_records_data = [
        (24410114, "Tran Tireu Thuan", 30),
        (24410100, "Nguyen Phuong Tan", 30),
        (24410109, "Nguyen Thi Thu Thao", 28),
        (24410092, "Huynh Duy Quoc", 35),
        (24410040, "Ha Huy Hung", 22)
    ]

    display(Markdown("### 👥 Records to Insert"))
    new_df = spark.createDataFrame(new_records_data, schema)

    # Display records in a styled table
    records_pandas = new_df.toPandas()
    styled_records = records_pandas.style.background_gradient(cmap='Greens', subset=['age']) \
                                        .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                        .set_caption("New Records to be Inserted")
    display(styled_records)

    try:
        # Method: Insert via Spark-Elasticsearch connector
        display(Markdown("### 🔄 Inserting Records..."))
        display(HTML("<div style='background-color: #fff3cd; padding: 10px; border-radius: 5px;'>⏳ Inserting records via Spark-Elasticsearch connector...</div>"))

        # Write DataFrame directly to Elasticsearch using Spark connector
        new_df.write \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .option("es.write.operation", "index") \
            .option("es.mapping.id", "id") \
            .mode("append") \
            .save()

        display(HTML("<div style='background-color: #d1ecf1; padding: 10px; border-radius: 5px;'>✅ Records inserted successfully using Spark connector!</div>"))

        # Wait for indexing with progress indicator
        display(HTML("<div style='background-color: #e2e3e5; padding: 10px; border-radius: 5px;'>⏱️ Waiting 3 seconds for Elasticsearch indexing...</div>"))
        time.sleep(3)

        # Verify insert by reading back the records
        display(Markdown("### 🔍 Verification"))
        try:
            verification_df = spark.read \
                .format("org.elasticsearch.spark.sql") \
                .option("es.nodes", "elasticsearch") \
                .option("es.port", "9200") \
                .option("es.resource", "2_people_data_2k") \
                .option("es.query", '{"terms":{"id":[24410114, 24410100, 24410109, 24410092, 24410040]}}') \
                .load()

            found_count = verification_df.count()

            if found_count > 0:
                display(HTML(f"<div style='background-color: #d4edda; padding: 10px; border-radius: 5px;'>✅ Found {found_count} inserted records:</div>"))

                # Display verified records
                verified_records = verification_df.select("id", "name", "age").orderBy("id").toPandas()
                styled_verified = verified_records.style.background_gradient(cmap='Blues', subset=['age']) \
                                                        .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                                        .set_caption("Successfully Inserted Records")
                display(styled_verified)

                # Create a simple visualization of inserted records
                fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

                # Age distribution of inserted records
                ax1.bar(range(len(verified_records)), verified_records['age'], color='lightgreen', alpha=0.7)
                ax1.set_xlabel('Record Index')
                ax1.set_ylabel('Age')
                ax1.set_title('Age of Inserted Records')
                ax1.set_xticks(range(len(verified_records)))
                ax1.set_xticklabels([f"ID {id}" for id in verified_records['id']], rotation=45, ha='right')

                # Age distribution histogram
                ax2.hist(verified_records['age'], bins=len(set(verified_records['age'])), alpha=0.7, color='skyblue', edgecolor='black')
                ax2.set_xlabel('Age')
                ax2.set_ylabel('Count')
                ax2.set_title('Age Distribution of Inserted Records')

                plt.tight_layout()
                plt.show()

            else:
                display(HTML("<div style='background-color: #f8d7da; padding: 10px; border-radius: 5px;'>⚠️ No records found - may need manual verification</div>"))

        except Exception as verify_error:
            error_html = f"""
            <div style='background-color: #f8d7da; border: 1px solid #f5c6cb; color: #721c24;
                        padding: 15px; border-radius: 5px;'>
                <h5 style='margin: 0;'>⚠️ Verification Error</h5>
                <p style='margin: 5px 0 0 0;'>{str(verify_error)}</p>
                <p style='margin: 5px 0 0 0;'><em>Records may have been inserted but verification failed</em></p>
            </div>
            """
            display(HTML(error_html))

        # Success message
        success_html = f"""
        <div style='background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724;
                    padding: 15px; border-radius: 5px; margin: 20px 0;'>
            <h4 style='margin: 0;'>✅ INSERT 5 RECORDS - COMPLETED SUCCESSFULLY!</h4>
            <p style='margin: 5px 0 0 0;'>Successfully inserted {len(new_records_data)} records into Elasticsearch index.</p>
            <ul style='margin: 10px 0 0 20px;'>
                <li>Student IDs: {', '.join([str(record[0]) for record in new_records_data])}</li>
                <li>Age range: {min([record[2] for record in new_records_data])} - {max([record[2] for record in new_records_data])}</li>
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
