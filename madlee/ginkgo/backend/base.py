from abc import ABC, abstractmethod, abstractproperty
from struct import unpack

from ..base import to_slot, list_slot


class BasicBackend(ABC):
    @abstractmethod
    def add_leaf(self, key, slot, size):
        '''Save the leaf into backend'''

    @abstractproperty
    def all_leaves(self):
        '''All leaves'''

    @abstractmethod
    def load_blocks(self, key, slot1, slot2):
        '''Load assigned blocks'''

    @abstractmethod
    def save_blocks(self, key, *blocks):
        '''Save back blocks'''

    @abstractmethod
    def commit(self):
        '''Save the pending data disk'''

    @abstractmethod
    def get_last_slot(self, key):
        '''Get last slot'''

    
    @abstractmethod
    def get_1st_slot(self, key):
        '''Get the 1st slot'''


    def prepare_blocks(self, key, blocks):
        result = []
        slot_type, size = self.all_leaves[key]
        for row in blocks:
            if row:
                start  = unpack('d', row[0:8])[0]
                finish = unpack('d', row[-size:-size+8])[0]
                slot   = to_slot(slot_type, ts=start)
                assert slot == to_slot(slot_type, ts=finish)
                result.append((slot, len(row)//size, start, finish, row))
        return result


    def save_records(self, key, records):
        blocks = []

        i = 0
        slot_type, _  = self.all_leaves[key]
        while i < len(records):
            i0 = i
            ts_i = unpack('d', records[i][:8])[0]
            slot = to_slot(slot_type,  ts=ts_i)
            j = len(records)
            while i < j:
                mid = (i+j)//2
                record_mid = records[mid]
                slot_mid = to_slot(slot_type, ts=unpack('d', record_mid[:8])[0])
                if slot_mid > slot:
                    j = mid
                else:
                    i = mid
            if i0 < i:
                blocks.append(b''.join(records[i0:i]))

        if blocks:
            self.save_blocks(key, blocks)
        return blocks

