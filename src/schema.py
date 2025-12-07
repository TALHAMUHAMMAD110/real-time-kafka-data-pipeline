from pyspark.sql.types import StructType, StringType, DoubleType, LongType

SENSOR_SCHEMA = StructType() \
    .add("sensorId", StringType()) \
    .add("value", DoubleType()) \
    .add("timestamp", LongType())