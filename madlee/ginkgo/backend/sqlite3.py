from sqlite3 import connect

from ...misc.file import join_path
from ...misc.db import execute_sqls
from .base import BasicBackEnd


SQL_PREPARE = '''CREATE TABLE IF NOT EXISTS ginkgo_leaf (
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    name CHAR(50) UNIQUE,
    slot CHAR(8),
    size INTEGER
);

CREATE TABLE IF NOT EXISTS ginkgo_blocks (
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    leaf INTEGER REFERENCES ginkgo_leaf(id),
    slot INTEGER,
    start DOUBLE, finish DOUBLE,
    data BINARY
);

CREATE UNIQUE INDEX IF NOT EXISTS ginkgo_index 
ON ginkgo_blocks (leaf, slot)
'''

SQL_LOAD_ALL_LEAVES = '''SELECT name, id, slot, size
FROM ginkgo_leaf
'''

SQL_ADD_LEAF = '''INSERT INTO ginkgo_leaf
( name, slot, size ) VALUES (?, ?, ? )
'''

SQL_SELECT_BLOCKS = '''SELECT data
FROM ginkgo_blocks
WHERE leaf = ? AND ? <= slot AND slot <= ?
'''

SQL_SAVE_BLOCKS = '''INSERT INTO ginkgo_blocks
(leaf, slot, start, finish, data) VALUES (?, ?, ?, ?, ?)
'''

class SqliteBackEnd(BasicBackEnd):
    def __init__(self, name, readonly=True):
        self.__db = db = connect(name)
        cursor = db.cursor()
        execute_sqls(SQL_PREPARE, cursor)
        leaves = list(cursor)
        self.__leaves = {
            row[0]: row[2:] for row in leaves
        }
        self.__leaf_ids = {
            row[0]: row[1] for row in leaves
        }


    @property
    def all_leaves(self):
        return {k: v[1:] for k, v in self.__leaves.items()}

    def add_leaf(self, key, slot, size):
        cursor = self.__db.cursor()
        cursor.execute(SQL_ADD_LEAF, (key, slot, size))
        self.__leaves[key] = (cursor.lastrowid, slot, size)
        self.__db.commit()


    def load_blocks(self, key, slot1, slot2):
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        cursor.execute(SQL_SELECT_BLOCKS, (leaf_id, slot1, slot2))
        return [row[0] for row in cursor]


    def save_blocks(self, key, *blocks):
        cursor = self.__db.cursor()
        leaf_id = self.__leaf_ids[key]
        blocks = [(leaf_id, row[0], row[1], row[2], row[3]) for row in blocks]
        cursor.executemany(SQL_SAVE_BLOCKS, )
