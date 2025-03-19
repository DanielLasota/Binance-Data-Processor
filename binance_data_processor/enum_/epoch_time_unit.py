from enum import Enum

class EpochTimeUnit(Enum):
    MILLISECONDS = 'ms'
    MICROSECONDS = 'us'

    @property
    def multiplier_of_second(self) -> int:
        return 1_000 if self == EpochTimeUnit.MILLISECONDS else 1_000_000

    def __mul__(self, seconds: float) -> int:
        return int(seconds * self.multiplier_of_second)

    def __rmul__(self, seconds: float) -> int:
        return int(seconds * self.multiplier_of_second)
