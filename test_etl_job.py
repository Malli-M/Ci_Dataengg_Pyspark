import pytest
from pyspark.sql import SparkSession
from etl_job import transform_data
import os

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local").appName("Test ETL").getOrCreate()

def test_transform_data(spark, tmp_path):
    input_path = tmp_path / "input.csv"
    output_path = tmp_path / "output"
    
    input_data = "id,amount\n1,50\n2,150\n3,200"
    input_path.write_text(input_data)
    
    # Perform the transformation
    transform_data(str(input_path), str(output_path))
    
    # Ensure the Spark context is still active here
    output_df = spark.read.option("header", "true").csv(str(output_path))
    
    # Perform assertions
    assert output_df.count() > 0  # Example assertion to ensure data is loaded
    output_data = output_df.collect()
    assert output_data[0]["amount"] == "150"  # Check transformed data

