from enum import Enum


class ContinuityEventType(Enum):
    START = 'start'
    STOP = 'stop'
    ERROR_CONTINUITY_LOST = 'error_continuity_lost'
    