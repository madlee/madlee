from conf.ginkgo import GINKGO_FOLDER 
from ...misc.file import join_path, is_file
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
    path = join_path(GINKGO_FOLDER, name)
    return SqliteBackend(path, readonly)

