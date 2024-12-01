from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(input_path, output_path):
    spark = SparkSession.builder.appName("ETL job").getOrCreate()
    
    # Extract: read CSV data
    df = spark.read.option("header", "true").csv(input_path)
    
    # Transformation: Filter and add 'double_amount' column
    transformed_df = (df.filter(col("amount") > 100)
                       .withColumn("double_amount", col("amount") * 2)
    )
    
    # Load: Write the transformed data to CSV
    transformed_df.write.mode("overwrite").csv(output_path, header=True)
    
    # Stop Spark session after all operations
    spark.stop()

if __name__ == "__main__":
    input_path = "data/input.csv"
    output_path = "data/output"
    transform_data(input_path, output_path)
