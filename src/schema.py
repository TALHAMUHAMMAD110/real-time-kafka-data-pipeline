from pyspark.sql.types import StructType, StringType, LongType

LOTTERY_SCHEMA = StructType() \
    .add("lottery_name", StringType()) \
    .add("event_type", StringType()) \
    .add("timestamp", LongType())