from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def transform_data(input_path, output_path):
  spark = SparkSession.builder.appName("ETL job").getOrCreate()
  #Extract: read csv data

  df = spark.read.option("header","true").csv(input_path)
  transformred_df = (df.filter(col("amount")>100)
                    .withColumn("double_amount", col("amount")*2)
                 )
  #load: write the transfomred data to CSV
  transformred_df.write.mode("overwrite").csv(output_path, header=True)
  spark.stop()


if __name__ == "__main__":
    input_path = "data/input.csv"
    output_path = "data/output"
    transform_data(input_path, output_path)
