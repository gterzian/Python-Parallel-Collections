import unittest
from itertools import chain, imap
from collections import defaultdict

from parallel_collections import ParallelList
        

class TestSequence(unittest.TestCase):
        
    def test_flatten(self):
        p = ParallelList([range(10000),range(10000)])
        self.assertEquals(p.flatten(), list(chain(*[range(10000),range(10000)])))
    
    def test_foreach(self):
        p = ParallelList([range(10000),range(10000)])
        self.assertEquals(list(p.foreach(double)), map(double, [range(10000),range(10000)]))
        self.assertTrue(p.foreach(double) is p)
        
    def test_map(self):
        p = ParallelList([range(10000),range(10000)])
        self.assertEquals(p.map(double), map(double, [range(10000),range(10000)]))
    
    def test_filter(self):
        p = ParallelList(['a','2','3'])
        pred = is_digit
        self.assertEquals(p.filter(pred), ParallelList(['2','3']))
        
    def test_flatmap(self):
        p = ParallelList([range(10000),range(10000)])
        self.assertEquals(p.flatmap(double), map(double, chain(*[range(10000),range(10000)])))
    
    def test_chaining(self):
        p = ParallelList([range(10000),range(10000)])
        i = ParallelList([range(10000),range(10000)])
        self.assertEquals(p.flatten().map(double), i.flatmap(double))
    
    def test_reduce(self):
        p = ParallelList(range(10000))
        self.assertEquals(49995000, p.reduce(add_up))
        p = ParallelList(['a', 'a', 'b'])
        self.assertEquals(dict(a=['a','a'], b=['b',]), p.reduce(group_letters, defaultdict(list)))

def is_digit(item):
    return item.isdigit()

def add_up(x,y):
    return x+y   
    
def group_letters(all, letter):
    all[letter].append(letter)
    return all

def double(item):
    return item * 2 

if __name__ == '__main__':
    unittest.main()