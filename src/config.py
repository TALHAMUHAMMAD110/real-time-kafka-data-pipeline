import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# KAFKA CONFIG
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:29092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "sensor_events")

# POSTGRES CONFIG
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB", "sensor_db")
POSTGRES_URL = os.getenv("POSTGRES_URL", f"jdbc:postgresql://postgres:5432/{POSTGRES_DB}")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE", "sensor_table")

# PGADMIN CONFIG
PGADMIN_DEFAULT_EMAIL = os.getenv("PGADMIN_DEFAULT_EMAIL")
PGADMIN_DEFAULT_PASSWORD = os.getenv("PGADMIN_DEFAULT_PASSWORD")

# CHECKPOINT LOCATION
CHECKPOINT_POSTGRES = os.getenv("CHECKPOINT_POSTGRES", "/home/jovyan/work/checkpoints/postgres")

# EMAIL CONFIG
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")