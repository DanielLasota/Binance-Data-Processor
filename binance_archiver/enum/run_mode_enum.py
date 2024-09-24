from enum import Enum, auto


class RunMode(Enum):
    DATA_SINK = auto()
    LISTENER = auto()
    ...
