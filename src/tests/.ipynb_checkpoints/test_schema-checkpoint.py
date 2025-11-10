import pytest
from pyspark.sql import SparkSession
from schema import SENSOR_SCHEMA
from pyspark.sql.types import StringType, DoubleType, LongType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Test") \
        .getOrCreate()

def test_sensor_schema_structure():
    """Test SENSOR_SCHEMA has correct structure"""
    assert len(SENSOR_SCHEMA.fields) == 3
    
    # Check field names and types
    field_names = [field.name for field in SENSOR_SCHEMA.fields]
    assert field_names == ["sensorId", "value", "timestamp"]
    
    field_types = [type(field.dataType) for field in SENSOR_SCHEMA.fields]
    assert field_types == [StringType, DoubleType, LongType]

def test_sensor_schema_validates_data(spark):
    """Test that valid data matches the schema"""
    valid_data = [
        ("sensor1", 10.5, 1672531200000),
        ("sensor2", 15.0, 1672531260000),
        ("sensor3", -5.5, 1672531320000),  # Negative value
    ]
    
    # This should not raise an exception
    df = spark.createDataFrame(valid_data, SENSOR_SCHEMA)
    assert df.count() == 3