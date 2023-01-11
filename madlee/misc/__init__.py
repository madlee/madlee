"""utility functions"""
from redis import StrictRedis as Redis

from .time import *
from .file import *
from .io   import *


def lazy_property(func):
    name = '_lazy_' + func.__name__
    @property
    def lazy(self):
        if hasattr(self, name):
            return getattr(self, name)
        else:
            value = func(self)
            setattr(self, name, value)
            return value
    return lazy
