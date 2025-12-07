import config

def test_config_values():
    """Test that config values are set"""
    assert hasattr(config, 'KAFKA_BROKER')
    assert hasattr(config, 'KAFKA_INPUT_TOPIC')
    assert hasattr(config, 'POSTGRES_USER')
    assert hasattr(config, 'POSTGRES_DB')
    assert hasattr(config, 'POSTGRES_URL')
    assert hasattr(config, 'POSTGRES_TABLE')
    assert hasattr(config, 'SENDER_EMAIL')
    assert hasattr(config, 'RECEIVER_EMAIL')
    
    # Test specific values if they're expected to be certain values
    assert config.KAFKA_INPUT_TOPIC == "sensor_events"
    assert config.POSTGRES_TABLE == "sensor_table"