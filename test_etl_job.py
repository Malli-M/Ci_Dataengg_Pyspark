import pytest
from pyspark.sql import SparkSession
from etl_job import transform_data  # Assuming the transform_data function is in etl_job.py
import os
import shutil

# Fixture to create a Spark session for the test
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local").appName("Test ETL").getOrCreate()
    yield spark_session
    # Make sure the Spark session is stopped after the test
    spark_session.stop()

# Test case for the transform_data function
def test_transform_data(spark):
    input_path = "input.csv"
    output_path = "output"
    
    # Create input CSV data
    input_data = "id,amount\n1,50\n2,150\n3,200"
    with open(input_path, "w") as f:
        f.write(input_data)
    
    # Perform the transformation
    transform_data(input_path, output_path)
    
    # Debugging: Ensure Spark session is still active before reading data
    assert spark.version is not None, "Spark session has been stopped prematurely"
    
    # Load the output data after transformation
    output_df = spark.read.option("header", "true").csv(output_path)
    
    # Perform assertions to check the transformed data
    assert output_df.count() > 0  # Ensure data is loaded
    output_data = output_df.collect()
    assert output_data[0]["amount"] == "150"  # Check transformed data (assuming transformation is doubling the amount)
    
    # Clean up the files after the test
    if os.path.exists(input_path):
        os.remove(input_path)
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
