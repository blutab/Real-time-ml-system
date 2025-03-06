from datetime import datetime

from pydantic import BaseModel


class Trade(BaseModel):
    """
    A trade from the Kraken trades API.

    "symbol": "MATIC/USD",
    "side": "sell",
    "price": 0.5117,
    "qty": 40.0,
    "ord_type": "market",
    "trade_id": 4665906,
    "timestamp": "2023-09-25T07:49:37.708706Z"
    """

    pair: str
    price: float
    volume: float
    timestamp: datetime
    timestamp_ms: int

    # TODO: let pydantic do the initialisation using timestamp
    # @field_validator('timestamp_ms', mode='after')
    # def set_timestamp_ms(cls, v, values) -> int:
    #     return int(values['timestamp'].timestamp() * 1000)

    def to_dict(self) -> dict:
        # pydantic method to convert model to dict
        # return self.model_dump_json()
        # return {
        #     "pair": self.pair,
        #     "price": self.price,
        #     "volume": self.volume,
        #     "timestamp": self.timestamp,
        #     "timestamp_ms": self.timestamp_ms
        # }
        return self.model_dump_json()
