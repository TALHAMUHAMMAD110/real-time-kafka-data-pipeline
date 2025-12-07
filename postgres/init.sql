CREATE TABLE IF NOT EXISTS sensor_data (
    id SERIAL PRIMARY KEY,
    sensorId VARCHAR(50) NOT NULL,
    windowStart TIMESTAMP NOT NULL,
    windowEnd TIMESTAMP NOT NULL,
    averageValue FLOAT(10) NOT NULL
);