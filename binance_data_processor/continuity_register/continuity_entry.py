from binance_data_processor.enums.continuity_event_type import ContinuityEventType


class ContinuityEntry:
    __slots__ = [
        'timestamp',
        'event_type',
        'instance_numer',
        'comment'
    ]

    def __init__(
            self,
            timestamp: str,
            event_type: ContinuityEventType,
            instance_numer: int,
            comment: str
    ):
        self.timestamp = timestamp
        self.instance_numer = instance_numer
        self.event_type = event_type
        self.comment = comment

    def __str__(self):
        return f'{self.timestamp} Instance ID {self.instance_numer} {self.event_type.value} {self.comment}'
