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

    # Create sample input data
    input_data = "id,amount\n1,50\n2,150\n3,200"
    input_path.write_text(input_data)

    # Run transformation
    transform_data(str(input_path), str(output_path))

    # Read output data
    output_df = spark.read.option("header", "true").csv(str(output_path))

    # Assert transformations
    assert output_df.count() == 2  # Only rows with amount > 100
    assert "double_amount" in output_df.columns
    assert output_df.filter(output_df["id"] == "2").collect()[0]["double_amount"] == "300"

    spark.stop()
