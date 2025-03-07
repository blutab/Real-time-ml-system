import json
from typing import List

from loguru import logger
from websocket import create_connection

from .trade import Trade


class KrakenWebsocketAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, pairs: List[str]):
        self.pairs = pairs

        # create a websocket client
        self._ws_client = create_connection(url=self.URL)

        # subscribe to the websocket
        self._subscribe()

    def _subscribe(self):
        """
        Subscribe to the Kraken websocket API and waits for the initial snapshot of the trades.
        """
        # send a subscribe message to the websocket
        self._ws_client.send(
            json.dumps(
                {
                    'method': 'subscribe',
                    'params': {
                        'channel': 'trade',
                        'symbol': self.pairs,
                        'snapshot': False,
                    },
                }
            )
        )

        for _pair in self.pairs:
            _ = self._ws_client.recv()
            _ = self._ws_client.recv()

    def get_trades(self):
        """
        Get the trades from the Kraken websocket API and return a list of Trade objects.

        Returns:
            List[Trade]: A list of Trade objects.
        """
        data = self._ws_client.recv()

        if 'heartbeat' in data:
            logger.info('Heartbeat received')
            return []

        # transform the raw string into a JSON object
        try:
            data = json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f'Error decoding JSON: {e}')
            return []

        # extract the data field from the JSON object
        try:
            trades_data = data['data']
        except KeyError as e:
            logger.error(f"No 'data' field with trades in the message: {e}")
            return []

        trades = [
            Trade(
                pair=trade['symbol'],
                price=trade['price'],
                volume=trade['qty'],
                timestamp=trade['timestamp'],
                timestamp_ms=self.datestr2milliseconds(trade['timestamp']),
            )
            for trade in trades_data
        ]

        return trades

    def datestr2milliseconds(self, iso_time: str) -> int:
        """
        Convert a ISO 8601 timestamp to milliseconds since epoch.
        """
        from datetime import datetime

        dt = datetime.strptime(iso_time, '%Y-%m-%dT%H:%M:%S.%fZ')
        return int(dt.timestamp() * 1000)
