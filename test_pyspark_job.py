import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark_job import clean_data

@pytest.fixture(scope="session")
def spark():

    session = SparkSession.builder \
        .appName("PySpark-CI-Testing") \
        .master("local[1]") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    yield session
    session.stop()

def test_clean_data(spark):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])


    test_data = [
        ("1", "Alice", 100.0), 
        ("2", "Bob", -10.0),   
        ("3", "Charlie", 0.0),  
        ("4", None, 50.0),       
    ]

    df = spark.createDataFrame(test_data, schema=schema)
    

    result_df = clean_data(df)
    results = result_df.collect()


    assert len(results) == 1

    valid_record = results[0]


    assert valid_record["id"] == "1"
    assert valid_record["name"] == "Alice"
    assert valid_record["amount"] == 100.0


    assert pytest.approx(valid_record["amount_with_tax"], 0.001) == 120.0
