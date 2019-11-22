'''A timeseries Database based on redis'''


from .sqlite3 import SqliteBackend
try:
    from .dj import DjangoBackend
except ImportError:
    pass


def connect_backend(name, readonly=True, style=None):
    return SqliteBackend(name, readonly)


