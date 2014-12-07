import unittest
from itertools import chain
from collections import defaultdict

from parallel_collections import parallel
      


class TestParallel(unittest.TestCase):
    
    def test_flatten(self):
        p = parallel([[0,1,2,3,4,5],[0,1,2,3,4,5]])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatten()), list(chain(*[[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        
    def test_foreach(self):
        p = parallel([[0,1,2,3,4,5],[0,1,2,3,4,5]])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEqual(list(p), list(map(double, [[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        self.assertTrue(p.foreach(double) is None)
        
    def test_map(self):
        p = parallel([[0,1,2,3,4,5],[0,1,2,3,4,5]])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEqual(list(mapped), list(map(double, [[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = parallel(['a','2','3'])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = parallel([[0,1,2,3,4,5],[0,1,2,3,4,5]])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatmap(double)), list(map(double, chain(*[[0,1,2,3,4,5],[0,1,2,3,4,5]]))))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_chaining(self):
        p = parallel([[0,1,2,3,4,5],[0,1,2,3,4,5]])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatten().map(double)), list(list(parallel((d for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]])).flatmap(double))))
    
    def test_reduce(self):
        p = parallel(['a', 'a', 'b'])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEqual(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
    
    def test_gen_func_flatten(self):
        def inner_gen():
            for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatten()), list(chain(*[[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        
    def test_gen_func_foreach(self):
        def inner_gen():
            for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEqual(list(p), list(map(double, [[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        self.assertTrue(p.foreach(double) is None)
        
    def test_gen_func_map(self):
        def inner_gen():
            for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEqual(list(mapped), list(map(double, [[0,1,2,3,4,5],[0,1,2,3,4,5]])))
        self.assertFalse(mapped is p)
    
    def test_gen_func_filter(self):
        def inner_gen():
            for d in ['a','2','3']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEqual(list(filtered), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_gen_func_flatmap(self):
        def inner_gen():
            for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatmap(double)), list(map(double, chain(*[[0,1,2,3,4,5],[0,1,2,3,4,5]]))))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_gen_func_chaining(self):
        def inner_gen():
            for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEqual(list(p.flatten().map(double)), list(parallel((d for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]])).flatmap(double)))
    
    def test_gen_func_reduce(self):
        def inner_gen():
            for d in ['a', 'a', 'b']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEqual(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
        

class TestFactories(unittest.TestCase):
    
    def test_returns_gen(self):
        p = parallel((d for d in [[0,1,2,3,4,5],[0,1,2,3,4,5]]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        def inner_gen():
            for i in range(10):
                yield i
        p = parallel(inner_gen())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
    
    def test_list(self):
        p = parallel(list())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(tuple())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(set())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
    
    def test_raises_exception(self):
        def inner_func():
            return 1
        class InnerClass(object):
            pass
        with self.assertRaises(TypeError):
            p = parallel(inner_func())
        with self.assertRaises(TypeError):
            p = parallel(1)
        with self.assertRaises(TypeError):
            p = parallel(InnerClass())
        
         
 
def to_upper(item):
    return item.upper()   

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