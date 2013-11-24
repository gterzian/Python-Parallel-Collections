from multiprocessing import Pool
from itertools import chain, imap
from functools import partial
from UserList import UserList
from UserDict import UserDict



class ParallelList(UserList):
    
    def __init__(self, *args, **kwargs):
        self.pool = Pool()
        self._chunksize = None
        super(ParallelList, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        return iter(self.data)
    
    @property        
    def chunksize(self):
        if not self._chunksize:
            if len(self)/4 > 1:
                self._chunksize = len(self)/4
            else:
                self._chunksize = 1
        return self._chunksize
            
    def foreach(self, func):
        self.data = self.pool.map(func, self.data, self.chunksize)
        return self
        
    def map(self, func):
        return ParallelList(self.pool.map(func, self.data, self.chunksize))
        
    def flatten(self):
        return ParallelList(chain(*self.data))
        
    def flatmap(self, func):
        data = self.flatten()
        return ParallelList(self.pool.map(func, data, self.chunksize))
        
    def reduce(self, function, initializer=None):
        it = iter(self.data)
        if initializer is None:
            try:
                initializer = next(it)
            except StopIteration:
                raise TypeError('reduce() of empty sequence with no initial value')
        accum_value = initializer
        for x in it:
            accum_value = function(accum_value, x)
        return accum_value

    