from abs import ABC, abstractmethod, abstractproperty


class BasicBackEnd(ABC):
    @abstractmethod
    def add_leaf(self, key, slot, size):
        '''Save the leaf into backend'''

    @abstractproperty
    def all_leaves(self):
        '''All leaves'''

    @abstractmethod
    def load_blocks(self, key, slot1, slot2):
        '''Load assigned blocks'''

