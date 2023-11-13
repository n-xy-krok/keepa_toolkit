from enum import Enum


class PriorityEnum(int, Enum):
    LOW = 0
    DEFAULT = 1
    HIGH = 2