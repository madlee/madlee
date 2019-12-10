from sqlite3 import connect

from ...misc.file import join_path
from ...misc.db import execute_sqls
from .base import BasicBackend


SQL_PREPARE = '''CREATE TABLE IF NOT EXISTS ginkgo_leaf (
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    name CHAR(50) UNIQUE,
    slot CHAR(8),
    size INTEGER
);

CREATE TABLE IF NOT EXISTS ginkgo_blocks (
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    leaf INTEGER REFERENCES ginkgo_leaf(id),
    slot INTEGER, size INTEGER, 
    start DOUBLE, finish DOUBLE,
    data BINARY,
    CONSTRAINT UNIQUE (leaf, slot)
);

CREATE UNIQUE INDEX IF NOT EXISTS ginkgo_index 
ON ginkgo_blocks (leaf, slot);


SELECT name, id, slot, size
FROM ginkgo_leaf;

'''

SQL_ADD_LEAF = '''INSERT INTO ginkgo_leaf
( name, slot, size ) VALUES (?, ?, ? )
'''

SQL_SELECT_BLOCKS = '''SELECT data
FROM ginkgo_blocks
WHERE leaf = ? AND ? <= slot AND slot <= ?
'''

SQL_SAVE_BLOCKS = '''INSERT OR REPLACE INTO ginkgo_blocks
(leaf, slot, size, start, finish, data) VALUES (?, ?, ?, ?, ?, ?)
'''

SQL_SELECT_1ST_SLOT = '''SELECT MIN(slot) 
FROM ginkgo_blocks
WHERE leaf = ?
'''

SQL_SELECT_LAST_SLOT = '''SELECT MAX(slot) 
FROM ginkgo_blocks
WHERE leaf = ?
'''


class SqliteBackend(BasicBackend):
    '''Save in Sqlite DB'''

    def __init__(self, name, readonly=True):
        self.__db = db = connect(join_path('GINKGO', name))
        cursor = db.cursor()
        execute_sqls(SQL_PREPARE, cursor)
        leaves = list(cursor)
        self.__leaves = {
            row[0]: ((int(row[2]) if row[2] not in 'Ymd' else row[2]), row[3]) for row in leaves
        }
        self.__leaf_ids = {
            row[0]: row[1] for row in leaves
        }


    @property
    def all_leaves(self):
        return self.__leaves

    def add_leaf(self, key, slot, size):
        cursor = self.__db.cursor()
        cursor.execute(SQL_ADD_LEAF, (key, slot, size))
        self.__leaves[key] = (slot, size)
        self.__leaf_ids[key] = cursor.lastrowid
        self.__db.commit()


    def load_blocks(self, key, slot1, slot2):
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        cursor.execute(SQL_SELECT_BLOCKS, (leaf_id, slot1, slot2))
        return [row[0] for row in cursor]


    def save_blocks(self, key, *blocks):
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        blocks = self.prepare_blocks(key, blocks)
        blocks = [(leaf_id, row[0], row[1], row[2], row[3], row[4]) for row in blocks]
        cursor.executemany(SQL_SAVE_BLOCKS, blocks)


    def get_last_slot(self, key):
        '''Get last slot'''
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        cursor.execute(SQL_SELECT_LAST_SLOT, (leaf_id, ))
        return cursor.fetchone()[0]

    
    def get_1st_slot(self, key):
        '''Get the 1st slot'''
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        cursor.execute(SQL_SELECT_1ST_SLOT, (leaf_id, ))
        return cursor.fetchone()[0]


    def commit(self):
        self.__db.commit()

