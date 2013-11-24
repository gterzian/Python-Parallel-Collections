import unittest
from itertools import chain, imap
from collections import defaultdict

from parallel_collections import ParallelSeq, BaseParallelCollection


class TestBaseClass(unittest.TestCase):
    
    def setUp(self):
        self.p = BaseParallelCollection([1,2,3])
    
    def test_getitem(self):
        self.assertEquals(1, self.p[0])
        
    def test_setitem(self):
        self.p[0] = 2
        self.assertEquals(2, self.p[0])
    
    def test_len(self):
        self.assertEquals(3, len(self.p))
        
    def test_contains(self):
        self.assertTrue(3 in self.p)
        
    def test_del(self):
        del self.p[1]
        self.assertFalse(1 in self.p)
        
        

class TestSequence(unittest.TestCase):
        
    def test_flatten(self):
        p = ParallelSeq([range(10),range(10)])
        self.assertEquals(list(p.flatten()), list(chain(*[range(10),range(10)])))
        
    def test_map(self):
        p = ParallelSeq([range(10),range(10)])
        self.assertEquals(list(p.map(double)), list(imap(double, [range(10),range(10)])))
        
    def test_flatmap(self):
        p = ParallelSeq([range(10),range(10)])
        self.assertEquals(list(p.flatmap(double)), list(imap(double, chain(*[range(10),range(10)]))))
    
    def test_chaining(self):
        p = ParallelSeq([range(10),range(10)])
        i = ParallelSeq([range(10),range(10)])
        self.assertEquals(list(p.flatten().map(double)), list(i.flatmap(double)))
    
    def test_reduce(self):
        p = ParallelSeq(range(10))
        self.assertEquals(45, p.reduce(add_up))
        p = ParallelSeq(['a', 'a', 'b'])
        self.assertEquals(dict(a=['a','a'], b=['b',]), p.reduce(group_letters, defaultdict(list)))


def add_up(x,y):
    return x+y   
    
def group_letters(all, letter):
    all[letter].append(letter)
    return all

def double(item):
    return item * 2 

if __name__ == '__main__':
    unittest.main()