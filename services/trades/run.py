from loguru import logger
from quixstreams import Application
from typing import Union
from kraken_api.mock import KrakenMockAPI
from kraken_api.websocket import KrakenWebsocketAPI

def main(
        kafka_broker_address: str,
        kafka_topic: str,
        kraken_api: Union[KrakenMockAPI, KrakenWebsocketAPI]
):
    """
    It does 2 things:
    1. Reads trades from the Kraken trades API 
    2. Pushes them to a Kafka topic.

    Args:
        kafka_broker_address: str
        kafka_topic: str
        kraken_api: Union[KrakenMockAPI, KrakenWebsocketAPI]
        
    Returns:
        None
    """
    logger.info("Start the trades service")

    # Initialize Quix streams application
    # This class handles all the low-level details to connect to Kafka
    app = Application(
        broker_address=kafka_broker_address, 
    )

    # Define a topic where we will push the trades
    topic = app.topic(name=kafka_topic, value_serializer='json')

    with app.get_producer() as producer:

        while True:
            trades = kraken_api.get_trades()

            for trade in trades:
                #serialize the trade to bytes
                print(trade)
                message = topic.serialize(
                    key=trade.pair,
                    value=trade.to_dict(),
                )

                #push the serialized message to topic
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )

                logger.info(f"Pushing trade to Kafka topic: {trade}")

if __name__ == "__main__":
    from config import config

    #Initialize the Kraken API
    kraken_api = KrakenWebsocketAPI(pairs=config.pairs)

    main(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic,
        kraken_api = kraken_api
        )
