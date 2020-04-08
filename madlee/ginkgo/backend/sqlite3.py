from sqlite3 import connect
from functools import lru_cache

from ...misc.file import join_path
from ...misc.db import execute_sqls
from .base import BasicBackend


SQL_PREPARE = '''CREATE TABLE IF NOT EXISTS ginkgo_branch (
    id INTEGER  PRIMARY KEY AUTOINCREMENT,
    name CHAR(200) UNIQUE,
    start INTEGER,
    finish INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ginkgo_branch_name 
ON ginkgo_branch (name);

CREATE INDEX IF NOT EXISTS idx_ginkgo_branch_start 
ON ginkgo_branch (start);

CREATE INDEX IF NOT EXISTS idx_ginkgo_branch_finish 
ON ginkgo_branch (finish);

CREATE TABLE IF NOT EXISTS ginkgo_leaves (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    branch INTEGER REFERENCES ginkgo_branch(id),
    slot   INTEGER,
    data   BINARY,
    UNIQUE (branch, slot)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ginkgo_index 
ON ginkgo_leaves (branch, slot);

'''

SQL_SELECT_BRANCH_ID = '''
SELECT id 
FROM ginkgo_branch
WHERE name = ?
'''

SQL_NEW_BRANCH = '''INSERT INTO ginkgo_branch
(name, start, finish, leaves) 
VALUES (?, 99999999999999, 0, 0)
'''


SQL_SELECT_LEAVES = '''SELECT slot, data 
FROM ginkgo_leaves
WHERE branch = ? AND slot IN (%s)
ORDER BY slot
'''

SQL_SELECT_RANGE = '''SELECT start, finish
FROM ginkgo_branch
WHERE id = ? 
'''

SQL_SELECT_BRANCHS_1 = '''SELECT name
FROM ginkgo_branch
WHERE start <= ? AND finish >= ?
'''


SQL_SELECT_BRANCHS_2 = '''SELECT name
FROM ginkgo_branch
WHERE start >= ? AND start <= ? OR
   finish >= ? AND finish <= ? OR
   start >= ? AND finish <= ? OR 
   start <= ? AND finish >= ? 
'''

SQL_UPSERT_LEAVES = '''
INSERT INTO ginkgo_leaves 
    (branch, slot, data)
    VALUES (?, ?, ?)
    ON CONFLICT (branch, slot) 
    DO UPDATE SET data=?
'''

SQL_UPDATE_BRANCH = '''
UPDATE ginkgo_branch
    SET start = MIN(start, ?), finish = MAX(finish, ?)
    WHERE id = ?
'''

class SqliteBackend(BasicBackend):
    '''Save in Sqlite DB'''

    def __init__(self, name, readonly=True):
        self.__db = db = connect(name+'.sql')
        self.__readonly = readonly


    def save(self, branch, *leafs):
        '''Save a set of leafs into branch'''
        assert not self.readonly
        bid = self.branch_id(branch)
        start = min([row[0] for row in leafs])
        finish = max([row[0] for row in leafs])
        parms = [ 
            (bid, slot, data, data)
            for slot, data in leafs
        ]
        cr = self.__db.cursor()
        cr.executemany(SQL_UPSERT_LEAVES, parms)
        cr.execute(SQL_UPDATE_BRANCH, (start, finish, bid, ))
        self.__db.commit()


    @lru_cache(50)
    def branch_id(self, branch):
        cr = self.__db.cursor()
        cr.execute(SQL_SELECT_BRANCH_ID, (branch, ))
        row = cr.fetchone()
        if row:
            return row[0]
        elif self.readonly:
            return None
        else:
            cr.execute(SQL_NEW_BRANCH, (branch, ))
            self.__db.commit()
            return cr.lastrowid


    def load(self, branch, *slots):
        ''' Get data between [slot1, slot2]'''
        bid = self.branch_id(branch)
        cr = self.__db.cursor()
        sql = SQL_SELECT_LEAVES % ', '.join(['?'] * len(slots))
        parms = (bid, ) + tuple(slots)
        cr.execute(sql, parms)
        return dict(cr.fetchall())


    def range(self, branch):
        '''Return start/end pair of branch'''
        cr = self.__db.cursor()
        bid = self.branch_id(branch)
        cr.execute(SQL_SELECT_RANGE, (bid, ))
        return cr.fetchone()


    def branches(self, ts1, ts2=None):
        '''Return branch names between ts1 and ts2'''
        cr = self.__db.cursor()
        if ts2:
            cr.execute(SQL_SELECT_BRANCHS_2, (ts1, ts2, ts1, ts2, ts1, ts2, ts1, ts2))
        else:
            cr.execute(SQL_SELECT_BRANCHS_1, (ts1, ts1))
        return [row[0] for row in cr.fetchall()]


    def readonly(self):
        '''Return True if it is readonly'''
        return self.__readonly


    @classmethod
    def create(cls, name):
        '''Initialize the database'''
        db = connect(name+'.sql')
        cr = db.cursor()
        execute_sqls(SQL_PREPARE, cr)
        db.commit()

