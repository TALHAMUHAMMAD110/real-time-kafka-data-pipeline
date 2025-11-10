import pytest
from pyspark.sql import SparkSession
from transform import parse_and_aggregate
from schema import SENSOR_SCHEMA
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import col

@pytest.fixture(scope="session")
def spark():
    """Create test Spark session"""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Test") \
        .config("spark.sql.shuffle.partitions", 1) \
        .getOrCreate()

@pytest.fixture
def sample_kafka_data(spark):
    """Create sample Kafka-like DataFrame"""
    data = [
        ('{"sensorId": "sensor1", "value": 10.5, "timestamp": 1672531200000}',),
        ('{"sensorId": "sensor1", "value": 20.5, "timestamp": 1672531205000}',),
        ('{"sensorId": "sensor2", "value": 15.0, "timestamp": 1672531200000}',),
        ('{"sensorId": "sensor1", "value": 30.5, "timestamp": 1672531260000}',),  # Next minute
    ]
    return spark.createDataFrame(data, ["value"])

def test_parse_and_aggregate_returns_dataframe(spark, sample_kafka_data):
    """Test that parse_and_aggregate returns a DataFrame"""
    # Mock the readStream to return our sample data
    class MockDataFrameReader:
        def format(self, fmt):
            return self
        
        def option(self, key, value):
            return self
        
        def load(self):
            return sample_kafka_data
    
    spark.readStream = MockDataFrameReader()
    
    # Call the function
    result = parse_and_aggregate(spark, "dummy-broker", "dummy-topic")
    
    # Assert it returns a DataFrame
    assert isinstance(result, type(sample_kafka_data))

def test_parse_and_aggregate_schema(spark, sample_kafka_data):
    """Test that the output has correct schema"""
    class MockDataFrameReader:
        def format(self, fmt):
            return self
        
        def option(self, key, value):
            return self
        
        def load(self):
            return sample_kafka_data
    
    spark.readStream = MockDataFrameReader()
    
    result = parse_and_aggregate(spark, "dummy-broker", "dummy-topic")
    
    # Check output columns
    expected_columns = {"sensorId", "windowStart", "windowEnd", "averageValue"}
    assert set(result.columns) == expected_columns
    
    # Check column types (basic check)
    assert str(result.schema["sensorId"].dataType) == "StringType()"
    assert str(result.schema["averageValue"].dataType) == "DoubleType()"
    assert str(result.schema["windowStart"].dataType) == "LongType()"