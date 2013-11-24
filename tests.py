import unittest
from itertools import chain, imap
from collections import defaultdict

from parallel_collections import ParallelList, ParallelDict
        

class TestList(unittest.TestCase):
        
    def test_flatten(self):
        p = ParallelList([range(10),range(10)])
        self.assertEquals(p.flatten(), list(chain(*[range(10),range(10)])))
    
    def test_foreach(self):
        p = ParallelList([range(10),range(10)])
        self.assertEquals(list(p.foreach(double)), map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is p)
        
    def test_map(self):
        p = ParallelList([range(10),range(10)])
        mapped = p.map(double)
        self.assertEquals(mapped, map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = ParallelList(['a','2','3'])
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(filtered, ParallelList(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = ParallelList([range(10),range(10)])
        self.assertEquals(p.flatmap(double), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_chaining(self):
        p = ParallelList([range(10),range(10)])
        i = ParallelList([range(10),range(10)])
        self.assertEquals(p.flatten().map(double), i.flatmap(double))
    
    def test_reduce(self):
        p = ParallelList(['a', 'a', 'b'])
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is p)
        
        
class TestDict(unittest.TestCase):
 
    def test_flatten(self):
        d = ParallelDict(zip(range(2), [[[1,2],[3,4]],[3,4]]))
        self.assertEqual(d.flatten(), ParallelDict(zip(range(2), [[1,2,3,4],[3,4]])))
        print d
        for i in d.flatten():
            print i
    
    def test_foreach(self):
        d = ParallelDict(zip([i for i in range(10)], range(10)))
        self.assertEquals(d.foreach(double_dict), ParallelDict(zip((double(i) for i in range(10)), (double(i) for i in range(10)))))
        self.assertTrue(d.foreach(double_dict) is d)
        
    def test_map(self):
        d = ParallelDict(zip([i for i in range(10)], range(10)))
        mapped = d.map(double_dict)
        self.assertEquals(mapped, ParallelDict(zip((double(i) for i in range(10)), (double(i) for i in range(10)))))
        self.assertFalse(mapped is d)
        
    def test_filter(self):
        p = ParallelDict(zip([i for i in range(3)], ['a','2', '3',]))
        pred = is_digit_dict
        self.assertEquals(p.filter(pred),  ParallelDict(zip([1,2,], ['2', '3'])))
        self.assertFalse(p.filter(pred) is p)
        
   

def is_digit(item):
    return item.isdigit()

def is_digit_dict(item):
    return item[1].isdigit()

def add_up(x,y):
    return x+y   
    
def group_letters(all, letter):
    all[letter].append(letter)
    return all

def double(item):
    return item * 2 
    
def double_dict(item):
    return [i * 2 for i in item]

if __name__ == '__main__':
    unittest.main()