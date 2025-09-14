from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from IPython.display import display, HTML, Markdown

# Setup for Jupyter
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def display_title(title):
    """Display a formatted title in Jupyter"""
    display(Markdown(f"## {title}"))

def create_spark_session():
    """Create and return Spark session"""
    return SparkSession.builder \
        .appName("GetAllData") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.14.3") \
        .getOrCreate()

def main():
    display_title("📊 1. GET ALL DATA FROM ELASTICSEARCH")

    # Create Spark Session
    spark = create_spark_session()
    print(f"✅ Application ID: {spark.sparkContext.applicationId}")

    try:
        # Read all data from people index
        display(Markdown("### 📋 Reading from 2_people_data_2k index"))
        people_df = spark.read \
            .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "elasticsearch") \
            .option("es.port", "9200") \
            .option("es.resource", "2_people_data_2k") \
            .load()

        total_count = people_df.count()
        display(HTML(f"<div style='background-color: #e8f5e8; padding: 10px; border-radius: 5px;'><b>📈 Total records: {total_count:,}</b></div>"))

        # Sample data with styled display
        display(Markdown("### 👥 Sample Data (first 20 records)"))
        sample_df = people_df.select("id", "name", "age").limit(20)
        sample_pandas = sample_df.toPandas()

        # Style the pandas DataFrame for better display
        styled_df = sample_pandas.style.background_gradient(cmap='Blues', subset=['age']) \
                                      .format({'id': '{:,.0f}', 'age': '{:.0f}'}) \
                                      .set_caption("Sample Data from 2_people_data_2k Index")
        display(styled_df)

        # Statistics with visual display
        display(Markdown("### 📊 Data Summary Statistics"))
        stats_df = people_df.agg(
            min("age").alias("min_age"),
            max("age").alias("max_age"),
            avg("age").alias("avg_age"),
            count("*").alias("total_records")
        ).collect()[0]

        # Create a nice stats display
        stats_html = f"""
        <div style='display: flex; gap: 20px; margin: 20px 0;'>
            <div style='background-color: #f0f8ff; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                <h3 style='color: #2c3e50; margin: 0;'>Min Age</h3>
                <p style='font-size: 24px; font-weight: bold; color: #e74c3c; margin: 5px 0;'>{stats_df['min_age']}</p>
            </div>
            <div style='background-color: #f0fff0; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                <h3 style='color: #2c3e50; margin: 0;'>Max Age</h3>
                <p style='font-size: 24px; font-weight: bold; color: #27ae60; margin: 5px 0;'>{stats_df['max_age']}</p>
            </div>
            <div style='background-color: #fff8f0; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                <h3 style='color: #2c3e50; margin: 0;'>Avg Age</h3>
                <p style='font-size: 24px; font-weight: bold; color: #f39c12; margin: 5px 0;'>{stats_df['avg_age']:.1f}</p>
            </div>
            <div style='background-color: #f8f0ff; padding: 15px; border-radius: 10px; text-align: center; flex: 1;'>
                <h3 style='color: #2c3e50; margin: 0;'>Total Records</h3>
                <p style='font-size: 24px; font-weight: bold; color: #8e44ad; margin: 5px 0;'>{stats_df['total_records']:,}</p>
            </div>
        </div>
        """
        display(HTML(stats_html))

        # Age distribution visualization
        display(Markdown("### 📈 Age Distribution Visualization"))

        # Get age distribution data
        age_dist = people_df.groupBy("age").count().orderBy("age").toPandas()

        # Create visualization
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Histogram
        ax1.hist(age_dist['age'], weights=age_dist['count'], bins=30, alpha=0.7, color='skyblue', edgecolor='black')
        ax1.set_xlabel('Age')
        ax1.set_ylabel('Count')
        ax1.set_title('Age Distribution - Histogram')
        ax1.grid(True, alpha=0.3)

        # Top 10 ages bar chart
        top_ages = age_dist.nlargest(10, 'count')
        bars = ax2.bar(top_ages['age'].astype(str), top_ages['count'], color='lightcoral', alpha=0.8)
        ax2.set_xlabel('Age')
        ax2.set_ylabel('Count')
        ax2.set_title('Top 10 Most Common Ages')
        ax2.tick_params(axis='x', rotation=45)

        # Add value labels on bars
        for bar in bars:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 1,
                    f'{int(height)}', ha='center', va='bottom')

        plt.tight_layout()
        plt.show()

        # Success message
        display(HTML("""
        <div style='background-color: #d4edda; border: 1px solid #c3e6cb; color: #155724;
                    padding: 15px; border-radius: 5px; margin: 20px 0;'>
            <h4 style='margin: 0;'>✅ GET ALL DATA - COMPLETED SUCCESSFULLY!</h4>
            <p style='margin: 5px 0 0 0;'>Successfully retrieved and analyzed {total_count:,} records from Elasticsearch.</p>
        </div>
        """.format(total_count=total_count)))

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
