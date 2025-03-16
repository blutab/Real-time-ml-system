from config import config
from loguru import logger
from quixstreams import Application
from quixstreams.sinks.core.csv import CSVSink


def main(
    kafka_broker_address: str,
    kafka_consumer_group: str,
    kafka_input_topic: str,
    feature_group_name: str,
    feature_group_version: int,
):
    """
    2 things:
    1. Read messages from kafka
    2. Write messages to feature store
    """
    logger.info('Hello from to-feature-store service')

    app = Application(
        broker_address=kafka_broker_address, consumer_group=kafka_consumer_group
    )

    input_topic = app.topic(kafka_input_topic, value_deserializer='json')

    # Push messages to feature store
    # Initialize a CSV sink with a file path
    csv_sink = CSVSink(path='technical_indicators.csv')

    sdf = app.dataframe(input_topic)

    # sdf = sdf.update(lambda value: logger.info(f'Candle: {value}'))

    # Do some processing here ...
    # Sink data to a CSV file
    sdf.sink(csv_sink)

    app.run()


if __name__ == '__main__':
    from config import config

    main(
        kafka_broker_address=config.kafka_broker_address,
        kafka_consumer_group=config.kafka_consumer_group,
        kafka_input_topic=config.kafka_input_topic,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
    )
