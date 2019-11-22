'''A timeseries Database based on redis'''


from .sqlite3 import SqliteBackEnd
try:
    from .dj import DjangoBackEnd
except ImportError:
    pass


def connect_backend(name, readonly=True, style=None):
    return SqliteBackEnd(name, readonly)


