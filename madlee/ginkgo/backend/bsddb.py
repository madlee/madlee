from lzma import decompress as decompress_lzma
import bsddb3 as bsddb


from madlee.misc.file import ensure_dirs, join_path, split_path
from ..const import GINKGO_FOLDER, BSDDB_MAX_OPEN_DB
from .base import BasicBackend



class BerkelyDBBackend(BasicBackend):
    def __init__(self, name, leafs, slot_func):
        self.__leafs = leafs
        self.__slot_func = slot_func

        folder, name = split_path(name)
        if folder:
            folder = join_path(GINKGO_FOLDER, folder)
        else:
            folder = GINKGO_FOLDER
        ensure_dirs(folder)

        self.__name = name
        full_path = join_path(folder, name)

        self.__dbenv = dbenv = bsddb.db.DBEnv()
        dbenv.open(full_path, bsddb.db.DB_CREATE | bsddb.db.DB_INIT_MPOOL)


    @property
    def name(self):
        return self.__name


    @property
    def slot_func(self):
        return self.__slot_func


    def get_leaf(self, name):
        kind, _ = name.split(':', 1)
        return self.__leafs[kind]


    def get_table(self, name, year):
        db = bsddb.db.DB(self.__dbenv)
        db.open('%s%d.bdb' % (self.__name, year), name, bsddb.db.DB_RDONLY)
        return db


    def get_slots(self, ts1, ts2, *names):
        assert ts1 <= ts2
        slot_func, _, compress,  = self.get_leaf(names[0])
        slot_func = self.slot_func[slot_func]
        slot1, slot2 = slot_func(ts1), slot_func(ts2)
        result = [self.get_record(name, slot1, slot2, ts1.year, ts2.year, compress) for name in names]
        return result


    def get_record(self, name, slot1, slot2, year1, year2, compress):
        if year1 == year2:
            db = self.get_table(name, year1)
            cursor = db.cursor()
            cursor.set_range(slot1)
            result = self.get_range(cursor, slot2)
        else:
            db = self.get_table(name, year1)
            cursor = db.cursor()
            cursor.set_range(slot1)
            result = self.get_range(cursor)
            for year in range(year1+1, year2):
                db = self.get_table(name, year)
                cursor = db.cursor()
                result += self.get_range(cursor)
            db = self.get_table(name, year1)
            cursor = db.cursor()
            result += self.get_range(cursor, slot2)
        
        if compress:
            result = [self.decompress(row, compress) for row in result]
        return result


    def get_range(self, cursor, slot_end=None):
        result = []
        record = cursor.current()
        while record:
            if slot_end and record[1] > slot_end:
                break
            else:
                result.append(record[1])
                record = cursor.next()
        return result


    def decompress(self, data, type):
        return decompress_lzma(data)

