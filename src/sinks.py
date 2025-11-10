from pyspark.sql.functions import col, from_unixtime
from config import *
import logging

def write_to_postgres(batch_df, _):
    url = POSTGRES_URL
    table = POSTGRES_TABLE
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD

    logging.info("Starting batch write to PostgreSQL...")
    logging.info(f"Target table: {table}, URL: {url}")

    try:
        # Convert epoch millis to proper timestamps
        batch_df_to_pg = (
            batch_df
            .withColumn("window_start", from_unixtime(col("window_start") / 1000).cast("timestamp"))
            .withColumn("window_end", from_unixtime(col("window_end") / 1000).cast("timestamp"))
        )

        record_count = batch_df_to_pg.count()
        logging.info(f"Batch contains {record_count} records to insert.")

        # Write to PostgreSQL
        batch_df_to_pg.write \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        logging.info(f"✅ Successfully wrote {record_count} records to PostgreSQL table '{table}'.")

    except Exception as e:
        logging.error(f"❌ Error writing batch to PostgreSQL: {e}")
        raise


def write_to_postgres_streaming(df, checkpoint):
    logging.info("Initializing streaming write to PostgreSQL...")
    logging.info(f"Checkpoint location: {checkpoint}")

    try:
        query = (
            df.writeStream
            .outputMode("update")
            .foreachBatch(write_to_postgres)
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="1 minute")
            .start()
        )

        logging.info("✅ PostgreSQL streaming sink started successfully.")
        return query

    except Exception as e:
        logging.error(f"❌ Failed to start PostgreSQL streaming sink: {e}")
        raise