from abc import ABC, abstractmethod, abstractproperty
from struct import unpack

from ..base import to_slot, list_slot


class BasicBackend(ABC):
    pass