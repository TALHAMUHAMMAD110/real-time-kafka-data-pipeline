from pyspark.sql.functions import from_json, col, window, avg, timestamp_millis,round
from schema import SENSOR_SCHEMA
import logging

def parse_and_aggregate(spark, broker, topic):
    logging.info("Starting parse_and_aggregate function.")
    logging.info(f"Connecting to Kafka broker: {broker}, topic: {topic}")
    
    try:
        raw = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", broker) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

            
        logging.info("Successfully connected to Kafka and started reading stream.")
        
    except Exception as e:
        logging.error(f"❌ Error reading from Kafka: {e}")
        raise

    try:
        parsed = raw.selectExpr("CAST(value AS STRING) as json_str") \
                    .select(from_json(col("json_str"), SENSOR_SCHEMA).alias("data")) \
                    .select("data.*") \
                    .withColumn("eventTime", timestamp_millis(col("timestamp")))
        
        logging.info("✅ Successfully parsed incoming messages.")
    except Exception as e:
        logging.error(f"❌ Error parsing Kafka messages: {e}")
        raise


    try:
        logging.info("Starting aggregation...")
        aggregated = parsed.withWatermark("eventTime", "2 minutes") \
                        .groupBy(col("sensorId"), window(col("eventTime"), "1 minute")) \
                        .agg(round(avg("value"), 2).alias("averageValue"))
                       
        logging.info("✅ Aggregation completed successfully.")
    except Exception as e:
        logging.error(f"❌ Error during aggregation: {e}")
        raise
    
    result = aggregated.select(
        col("sensorId"),
        (col("window.start").cast("long") * 1000).alias("windowStart"),
        (col("window.end").cast("long") * 1000).alias("windowEnd"),
        col("averageValue")
    )
    
    logging.info("parse_and_aggregate function completed successfully.")
    return result
