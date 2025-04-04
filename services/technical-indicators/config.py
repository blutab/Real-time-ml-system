from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env')
    kafka_broker_address: str
    kafka_consumer_group: str
    kafka_input_topic: str
    kafka_output_topic: str
    max_candles_in_state: int
    candle_seconds: int


config = Config()
