import unittest
from itertools import chain, imap
from collections import defaultdict

from parallel_collections import parallel, lazy_parallel, parallel_gen
      

class TestList(unittest.TestCase):
        
    def test_flatten(self):
        p = parallel([range(10),range(10)])
        self.assertEquals(p.flatten(), list(chain(*[range(10),range(10)])))
    
    def test_foreach(self):
        p = parallel([range(10),range(10)])
        p.foreach(double)
        self.assertEquals(p, map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is None)
        
    def test_map(self):
        p = parallel([range(10),range(10)])
        mapped = p.map(double)
        self.assertEquals(mapped, map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = parallel(['a','2','3'])
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(filtered, list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = parallel([range(10),range(10)])
        self.assertEquals(p.flatmap(double), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_chaining(self):
        p = parallel([range(10),range(10)])
        self.assertEquals(p.flatten().map(double), p.flatmap(double))
    
    def test_reduce(self):
        p = parallel(['a', 'a', 'b'])
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is p)


class TestGen(unittest.TestCase):
    
    def test_flatten(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten()), list(chain(*[range(10),range(10)])))
        
    def test_foreach(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEquals(list(p), map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is None)
        
    def test_map(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEquals(list(mapped), map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = parallel((d for d in ['a','2','3']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatmap(double)), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_chaining(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten().map(double)), list(parallel((d for d in [range(10),range(10)])).flatmap(double)))
    
    def test_reduce(self):
        p = parallel((d for d in ['a', 'a', 'b']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
    
    def test_gen_func_flatten(self):
        def inner_gen():
            for d in [range(10),range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten()), list(chain(*[range(10),range(10)])))
        
    def test_gen_func_foreach(self):
        def inner_gen():
            for d in [range(10),range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEquals(list(p), map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is None)
        
    def test_gen_func_map(self):
        def inner_gen():
            for d in [range(10),range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEquals(list(mapped), map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_gen_func_filter(self):
        def inner_gen():
            for d in ['a','2','3']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_gen_func_flatmap(self):
        def inner_gen():
            for d in [range(10),range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatmap(double)), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_gen_func_chaining(self):
        def inner_gen():
            for d in [range(10),range(10)]:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten().map(double)), list(parallel((d for d in [range(10),range(10)])).flatmap(double)))
    
    def test_gen_func_reduce(self):
        def inner_gen():
            for d in ['a', 'a', 'b']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
        
    def test_lazy_flatten(self):
        p = lazy_parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten()), list(chain(*[range(10),range(10)])))
    
    def test_lazy_foreach(self):
        p = lazy_parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p.foreach(double)
        self.assertEquals(list(p), map(double, [range(10),range(10)]))
        self.assertTrue(p.foreach(double) is None)
        
    def test_lazy_map(self):
        p = lazy_parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        mapped = p.map(double)
        self.assertEquals(list(mapped), map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_lazy_filter(self):
        p = lazy_parallel(['a','2','3'])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_lazy_flatmap(self):
        p = lazy_parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatmap(double)), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_lazy_chaining(self):
        p = lazy_parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatten().map(double)), list(parallel((d for d in [range(10),range(10)])).flatmap(double)))
    
    def test_lazy_reduce(self):
        p = lazy_parallel(['a', 'a', 'b'])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
        
class TestClosure(unittest.TestCase):

    def test_closure_flatten(self):
        p = parallel_gen([range(10),range(10)])
        self.assertEquals(list(p.flatten().result()), list(chain(*[range(10),range(10)])))
        
    def test_closure_map(self):
        p = parallel_gen([range(10),range(10)])
        mapped = p.map(double)
        self.assertEquals(list(mapped.result()), map(double, [range(10),range(10)]))
        self.assertFalse(mapped is p)
    
    def test_closure_filter(self):
        p = parallel_gen(['a','2','3'])
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(list(filtered.result()), list(['2','3']))
        self.assertFalse(filtered is p)
        
    def test_closure_flatmap(self):
        p = parallel_gen([range(10),range(10)])
        self.assertEquals(list(p.flatmap(double).result()), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)
    
    def test_closure_chaining(self):
        p = parallel_gen([range(10),range(10)])
        self.assertEquals(list(p.flatten().map(double).result()), list(parallel_gen((d for d in [range(10),range(10)])).flatmap(double).result()))
    
    def test_closure_reduce(self):
        p = parallel_gen(['a', 'a', 'b'])
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)
        
        
class TestDict(unittest.TestCase):
 
    def test_flatten(self):
        d = parallel(dict(zip(range(2), [[[1,2],[3,4]],[3,4]])))
        self.assertEqual(d.flatten(), dict(zip(range(2), [[1,2,3,4],[3,4]])))
    
    def test_foreach(self):
        d = parallel(dict(zip(range(10), range(10))))
        d.foreach(double_dict)
        self.assertEquals(d, dict(zip(range(10), (double(i) for i in range(10)))))
        self.assertTrue(d.foreach(double_dict) is None)
        
    def test_map(self):
        d = parallel(dict(zip(range(10), range(10))))
        mapped = d.map(double_dict)
        self.assertEquals(mapped, dict(zip(range(10), (double(i) for i in range(10)))))
        self.assertFalse(mapped is d)
        
    def test_filter(self):
        p = parallel(dict(zip(range(3), ['a','2', '3',])))
        pred = is_digit_dict
        self.assertEquals(p.filter(pred),  dict(zip([1,2,], ['2', '3'])))
        self.assertFalse(p.filter(pred) is p)
        
    def test_flatmap(self):
        d = parallel(dict(zip(range(2), [[[1,2],[3,4]],[3,4]])))
        flat_mapped = d.flatmap(double_dict)
        self.assertEquals(flat_mapped, dict(zip(range(2), [[2,4,6,8],[6,8]])))
        self.assertFalse(flat_mapped is d)
    
    def test_chaining(self):
        d = parallel(dict(zip(range(2), [[[1,2],[3,4]],[3,4]])))
        self.assertEquals(d.flatten().map(double_dict), d.flatmap(double_dict))
        
    def test_reduce(self):
        d = parallel(dict(zip(range(3),['a', 'a', 'b'])))
        reduced = d.reduce(group_letters_dict, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is d)
        

class TestString(unittest.TestCase):
        
    def test_flatten(self):
        p = parallel('qwerty')
        self.assertEquals(p.flatten(), 'qwerty')
    
    def test_foreach(self):
        p = parallel('qwerty')
        p.foreach(to_upper)
        self.assertEquals(p, ''.join(map(to_upper, 'qwerty')))
        self.assertEquals(p, 'QWERTY')
        self.assertTrue(p.foreach(double) is None)
        
    def test_map(self):
        p = parallel('qwerty')
        mapped = p.map(to_upper)
        self.assertEquals(mapped, ''.join(map(to_upper, 'qwerty')))
        self.assertFalse(mapped is p)
    
    def test_filter(self):
        p = parallel('a23')
        pred = is_digit
        filtered = p.filter(pred)
        self.assertEquals(filtered, '23')
        self.assertFalse(filtered is p)
        
    def test_flatmap(self):
        p = parallel('a23')
        self.assertEquals(p.flatmap(to_upper), ''.join(map(to_upper, chain(*'a23'))))
        self.assertFalse(p.flatmap(to_upper) is p)
    
    def test_chaining(self):
        p = parallel('a23')
        self.assertEquals(p.filter(is_digit).map(to_upper), p.filter(is_digit).flatmap(to_upper))
    
    def test_reduce(self):
        p = parallel('aab')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), reduced)
        self.assertFalse(reduced is p)
        

def _print(item):
    print item 
    return item       
 
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