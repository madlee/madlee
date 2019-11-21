'''A timeseries Database based on redis'''


from .sqlite3 import SqliteBackEnd
try:
    from .dj import DjangoBackEnd
except ImportError:
    pass


def create_backend(name, mode='r', style=None):
    return SqliteBackEnd(name, mode='r')


