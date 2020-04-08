from abc import ABC, abstractmethod, abstractclassmethod
from struct import unpack

from ..base import to_slot, list_slot


class BasicBackend(ABC):
    @abstractmethod
    def save(self, branch, *leafs):
        '''Save a set of leafs into end of branch'''


    @abstractmethod
    def load(self, branch, slot1, slot2):
        ''' Get data between [slot1, slot2]'''


    @abstractmehthod
    def range(self, branch):
        '''Return start/end pair of branch'''


    @abstractmethod
    def branches(self, slot1, slot2):
        '''Return branch names between slot1 and slot2'''


    @abstractproperty
    def readonly(self):
        '''Return True if it is readonly'''


    @abstractclassmethod
    def create(cls, name):
        '''Initialize the database'''

