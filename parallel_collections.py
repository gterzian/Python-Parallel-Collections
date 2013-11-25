from concurrent import futures
from itertools import chain, imap
from functools import partial
from UserList import UserList
from UserDict import UserDict


Pool = futures.ProcessPoolExecutor()

class Filter(object):
    
    def __init__(self, pred):
        self.pred = pred
        
    def __call__(self, item):
        if self.pred(item):
            return item


class ParallelSeq(object):
    
    @property        
    def chunksize(self):
        if not self._chunksize:
            if len(self)/4 > 1:
                self._chunksize = len(self)/4
            else:
                self._chunksize = 1
        return self._chunksize
    

class ParallelList(UserList, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self._chunksize = None
        self.pool = Pool
        super(ParallelList, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        return iter(self.data)
        
    def foreach(self, func):
        self.data = list(self.pool.map(func, self))
        return self
    
    def filter(self, pred):
        _filter = Filter(pred)
        return ParallelList(i for i in self.pool.map(_filter, self, ) if i)
        
    def map(self, func):
        return ParallelList(self.pool.map(func, self, ))
        
    def flatten(self):
        return ParallelList(chain(*self))
        
    def flatmap(self, func):
        data = self.flatten()
        return ParallelList(self.pool.map(func, data, ))
        
    def reduce(self, function, initializer=None):
        return reduce(function, self, initializer)
        
        
class ParallelDict(UserDict, ParallelSeq):
    
    def __init__(self, *args, **kwargs):
        self._chunksize = None
        self.pool = Pool
        super(ParallelDict, self).__init__(*args, **kwargs)
        
    def __iter__(self):
        for i in self.data.items():
            yield i
            
    def foreach(self, func):
        self.data =  dict(self.pool.map(func, self, ))
        return self
    
    def filter(self, pred):
        _filter = Filter(pred)
        return ParallelDict(i for i in self.pool.map(_filter, self, ) if i)
        
    def map(self, func):
        return ParallelDict(self.pool.map(func, self, ))
        
    def flatten(self):
        flat = []
        for i in self:
            try:
                flat.append((i[0],list(chain(*i[1]))))
            except TypeError:
                flat.append((i[0], i[1]))
        return ParallelDict(flat)
        
    def flatmap(self, func):
        data = self.flatten()
        return ParallelDict(self.pool.map(func, data, ))
        
    def reduce(self, function, initializer=None):
        it = iter(self)
        if initializer is None:
            try:
                initializer = next(it)
            except StopIteration:
                raise TypeError('reduce() of empty sequence with no initial value')
        accum_value = initializer
        for x in it:
            accum_value = function(accum_value, x)
        return accum_value
