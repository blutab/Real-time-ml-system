## Trades service

It ingests trades from the Kraken trades API and pushes them to a Kafka topic.
We use redpanda as our kafka provider and quixstreams to read and write to kafka.
We use Kraken API and websocket to get the trades data.
