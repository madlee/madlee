from .sqlite3 import SqliteBackend

try:
    from .dj import DjangoBackend
except ImportError:
    pass

try:
    from .bsddb import BerkelyDBBackend
except ImportError:
    pass

def connect_backend(name, readonly=True, style=None):

    return SqliteBackend(name, readonly)


