from pyspark.sql import SparkSession
from transform import parse_and_aggregate
from sinks import write_to_postgres_streaming
from config import *
from monitoring import send_failure_email
import traceback
import logging

def start_streaming_job():
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    # Initialize Spark
    spark = SparkSession.builder.appName("RealTimeDataPipeLineForSensorData").getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    # Transform stream
    df = parse_and_aggregate(spark, KAFKA_BROKER, KAFKA_INPUT_TOPIC)
    
    # Write to PostgreSQL
    write_to_postgres_streaming(df, CHECKPOINT_POSTGRES)
    
    spark.streams.awaitAnyTermination()
    
    
if __name__ == "__main__":
    try:
        start_streaming_job()
    except Exception as e:
        error_message = traceback.format_exc()
        send_failure_email(error_message)