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
        p.foreach(double)
        self.assertEquals(p, map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is None)
        
    def test_map(self):
        p = ParallelList([range(10),range(10)])
        mapped = p.map(double)
        self.assertEquals(mapped, map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = ParallelList(['a','2','3'])
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(filtered, list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = ParallelList([range(10),range(10)])
        self.assertEquals(p.flatmap(double), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_chaining(self):
        p = ParallelList([range(10),range(10)])
        self.assertEquals(p.flatten().map(double), p.flatmap(double))
    
    def test_reduce(self):
        p = ParallelList(['a', 'a', 'b'])
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is p)
        
        
class TestDict(unittest.TestCase):
 
    def test_flatten(self):
        d = ParallelDict(zip(range(2), [[[1,2],[3,4]],[3,4]]))
        self.assertEqual(d.flatten(), dict(zip(range(2), [[1,2,3,4],[3,4]])))
    
    def test_foreach(self):
        d = ParallelDict(zip(range(10), range(10)))
        d.foreach(double_dict)
        self.assertEquals(d, dict(zip(range(10), (double(i) for i in range(10)))))
        self.assertTrue(d.foreach(double_dict) is None)
        
    def test_map(self):
        d = ParallelDict(zip(range(10), range(10)))
        mapped = d.map(double_dict)
        self.assertEquals(mapped, dict(zip(range(10), (double(i) for i in range(10)))))
        self.assertFalse(mapped is d)
        
    def test_filter(self):
        p = ParallelDict(zip(range(3), ['a','2', '3',]))
        pred = is_digit_dict
        self.assertEquals(p.filter(pred),  dict(zip([1,2,], ['2', '3'])))
        self.assertFalse(p.filter(pred) is p)
        
    def test_flatmap(self):
        d = ParallelDict(zip(range(2), [[[1,2],[3,4]],[3,4]]))
        flat_mapped = d.flatmap(double_dict)
        self.assertEquals(flat_mapped, dict(zip(range(2), [[2,4,6,8],[6,8]])))
        self.assertFalse(flat_mapped is d)
    
    def test_chaining(self):
        d = ParallelDict(zip(range(2), [[[1,2],[3,4]],[3,4]]))
        self.assertEquals(d.flatten().map(double_dict), d.flatmap(double_dict))
        
    def test_reduce(self):
        d = ParallelDict(zip(range(3),['a', 'a', 'b']))
        reduced = d.reduce(group_letters_dict, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is d)
        
        
   

def is_digit(item):
    return item.isdigit()

def is_digit_dict(item):
    return item[1].isdigit()

def add_up(x,y):
    return x+y   
    
def group_letters(all, letter):
    all[letter].append(letter)
    return all

def group_letters_dict(all, letter):
    letter = letter[1]
    all[letter].append(letter)
    return all

def double(item):
    return item * 2 
    
def double_dict(item):
    k,v = item
    try:
        return [k, [i *2 for i in v]]
    except TypeError:
        return [k, v * 2]

if __name__ == '__main__':
    unittest.main()