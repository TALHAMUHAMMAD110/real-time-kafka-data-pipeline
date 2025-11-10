import pytest
from pyspark.sql import SparkSession
from schema import LOTTERY_SCHEMA
from pyspark.sql.types import StringType, LongType

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("Test") \
        .getOrCreate()

def test_lottery_schema_structure():
    """Test LOTTERY_SCHEMA has correct structure"""
    assert len(LOTTERY_SCHEMA.fields) == 3
    
    # Check field names and types
    field_names = [field.name for field in LOTTERY_SCHEMA.fields]
    assert field_names == ["lottery_name", "event_type", "timestamp"]
    
    field_types = [type(field.dataType) for field in LOTTERY_SCHEMA.fields]
    assert field_types == [StringType, StringType, LongType]

def test_lottery_schema_validates_data(spark):
    """Test that valid data matches the schema"""
    valid_data = [
        ("abc_lottery", 'clicked', 1672531200000),
        ("xyz_jackpot", 'pay', 1672531260000),
        ("test_lottery", 'register', 1672531320000), 
    ]
    
    # This should not raise an exception
    df = spark.createDataFrame(valid_data, LOTTERY_SCHEMA)
    assert df.count() == 3