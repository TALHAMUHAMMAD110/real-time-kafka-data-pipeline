CREATE TABLE IF NOT EXISTS lottery_aggregates (
    id SERIAL PRIMARY KEY,
    lottery_name VARCHAR(100) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    event_count INTEGER NOT NULL
);