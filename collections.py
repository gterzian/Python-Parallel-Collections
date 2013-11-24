from multiprocessing import Pool
from itertools import chain, imap


class ParallelSeq(object):
    
    def __init__(self, iterable=None):
        self.iterable = iterable or list()
        self.pool = Pool()
        
    def __iter__(self):
        for i in self.iterable:
            yield i
        
    def map(self, func):
        iterable = list(self)
        if len(iterable)/4 > 1:
            chunksize = len(iterable)/10
        else:
            chunksize = 1
        self.iterable = self.pool.imap(func, iterable, chunksize)
        return self
        
    def flatten(self):
        self.iterable = chain(*self.iterable)
        return self
        
    def flatmap(self, func):
        iterable = list(self.flatten())
        if len(iterable)/4 > 1:
            chunksize = len(iterable)/10
        else:
            chunksize = 1
        self.iterable = self.pool.imap(func, iterable, chunksize)
        return self

def double(item):
    return item * 2    
        
if __name__ == '__main__':
    print 'starting'
    p = ParallelSeq(iterable=[range(10),range(10)])
    for i in p.map(double):
        print i
    print 'flatten'
    p = ParallelSeq(iterable=[range(10),range(10)])
    for i in p.flatten():
        print i
    print 'flatmap'
    p = ParallelSeq(iterable=[range(10),range(10)])
    for i in p.flatmap(double):
        print i
    print 'compare'
    p = ParallelSeq(iterable=[range(1000),range(1000)])
    i = ParallelSeq(iterable=[range(1000),range(1000)])
    for x,y  in zip(imap(double, p.flatten()), i.flatmap(double)):
        assert x == y
    print 'chaining'
    p = ParallelSeq(iterable=[range(1000),range(1000)])
    i = ParallelSeq(iterable=[range(1000),range(1000)])
    for x,y  in zip(p.flatten().map(double), i.flatmap(double)):
        assert x == y
  
     
    