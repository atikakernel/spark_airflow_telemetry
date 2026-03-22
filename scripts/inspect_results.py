from pyspark.sql import SparkSession
import os

PROJECT_ROOT = "/home/diegokernel/proyectos/spark_airflow_telemetry"
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data/processed/vehicle_telemetry_agg")

def inspect_data():
    spark = SparkSession.builder \
        .appName("Inspect Processed Data") \
        .getOrCreate()
    
    print(f"Reading processed data from {OUTPUT_PATH}...")
    df = spark.read.parquet(OUTPUT_PATH)
    
    print("\n--- Schema ---")
    df.printSchema()
    
    print("\n--- Sample Data (First 10 rows) ---")
    df.show(10)
    
    print(f"\nTotal rows: {df.count()}")
    spark.stop()

if __name__ == "__main__":
    inspect_data()
