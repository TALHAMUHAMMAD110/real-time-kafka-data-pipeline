import config

def test_config_values():
    """Test that config values are set"""
    assert hasattr(config, 'KAFKA_BROKER')
    assert hasattr(config, 'KAFKA_INPUT_TOPIC')
    assert hasattr(config, 'KAFKA_OUTPUT_TOPIC')
    assert hasattr(config, 'MONGO_URI')
    assert hasattr(config, 'MONGO_DB')
    assert hasattr(config, 'MONGO_COLLECTION')
    assert hasattr(config, 'CHECKPOINT_KAFKA')
    assert hasattr(config, 'CHECKPOINT_MONGO')
    
    # Test specific values if they're expected to be certain values
    assert config.KAFKA_INPUT_TOPIC == "sensor-input"
    assert config.KAFKA_OUTPUT_TOPIC == "sensor-output"