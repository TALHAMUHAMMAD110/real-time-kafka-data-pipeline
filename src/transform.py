from pyspark.sql.functions import from_json, col, window, count, timestamp_millis
from schema import LOTTERY_SCHEMA
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
        parsed = (
            raw.selectExpr("CAST(value AS STRING) as json_str")
            .select(from_json(col("json_str"), LOTTERY_SCHEMA).alias("data"))
            .select("data.*")
            .withColumn("eventTime", timestamp_millis(col("timestamp")))
        )
        logging.info("✅ Successfully parsed incoming messages.")
    except Exception as e:
        logging.error(f"❌ Error parsing Kafka messages: {e}")
        raise


    try:
        logging.info("Starting aggregation...")
        aggregated = (
            parsed.withWatermark("eventTime", "2 minutes")
            .groupBy(
                col("lottery_name"),
                col("event_type"),
                window(col("eventTime"), "1 minute"),
            )
            .agg(count("*").alias("event_count"))
        )
        
        logging.info("✅ Aggregation completed successfully.")
    except Exception as e:
        logging.error(f"❌ Error during aggregation: {e}")
        raise
    
    result = aggregated.select(
        col("lottery_name"),
        col("event_type"),
        (col("window.start").cast("long") * 1000).alias("window_start"),
        (col("window.end").cast("long") * 1000).alias("window_end"),
        col("event_count"),
    )
    
    logging.info("parse_and_aggregate function completed successfully.")
    return result
