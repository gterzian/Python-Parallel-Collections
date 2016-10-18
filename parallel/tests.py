import unittest
from itertools import chain, imap
from collections import defaultdict

from parallel_collections import (
    parallel, lazy_parallel, ParallelSeq,
    ParallelList, ParallelDict,
    ParallelString, _Reducer, _Filter
)


class TestHelpers(unittest.TestCase):

    def test_reducer_two_items(self):
        reducer = _Reducer(group_letters, defaultdict(list))
        reducer('a')
        reducer('a')
        self.assertEquals(reducer.result, {'a': ['a', 'a']})

    def test_reducer_three_items(self):
        reducer = _Reducer(group_letters, defaultdict(list))
        reducer('a')
        reducer('a')
        reducer('b')
        self.assertEquals(reducer.result, {'a': ['a', 'a'], 'b': ['b']})

    def test_filter_none(self):
        _filter = _Filter(is_digit)
        self.assertEquals(_filter('a'), (False, None))

    def test_filter_returns_passing_item(self):
        _filter = _Filter(is_digit)
        self.assertEquals(_filter('1'), (True, '1'))

class TestGen(unittest.TestCase):

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

    def test_filter_with_ret_none_func(self):
        p = parallel((d for d in [True, False]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = ret_none
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list([]))
        self.assertFalse(filtered is p)

    def test_filter_with_none_as_func(self):
        p = parallel((d for d in [False, True]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = None
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list([True]))
        self.assertFalse(filtered is p)

    def test_filter_for_none_false_elements(self):
        p = parallel((d for d in [False, True, None]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        pred = is_none_or_false
        filtered = p.filter(pred)
        self.assertEquals(list(filtered), list([False, None]))
        self.assertFalse(filtered is p)

    def test_flatmap(self):
        p = parallel([range(10),range(10)])
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.flatmap(double_iterables)), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)

    def test_chaining(self):
        p = parallel((d for d in chain(range(10),range(10))))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.filter(is_even).map(double)), list(parallel([range(10),range(10)]).flatmap(double_evens_in_iterables)))

    def test_reduce(self):
        p = parallel((d for d in ['a', 'a', 'b']))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)

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
        self.assertEquals(list(p.flatmap(double_iterables)), map(double, chain(*[range(10),range(10)])))
        self.assertFalse(p.flatmap(double) is p)

    def test_gen_func_chaining(self):
        def inner_gen():
            for d in chain(*[range(10),range(10)]):
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        self.assertEquals(list(p.filter(is_even).map(double)), list(parallel([range(10),range(10)]).flatmap(double_evens_in_iterables)))

    def test_gen_func_reduce(self):
        def inner_gen():
            for d in ['a', 'a', 'b']:
                yield d
        p = parallel(inner_gen)
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        reduced = p.reduce(group_letters, defaultdict(list))
        self.assertEquals(dict(a=['a','a'], b=['b',]), dict(reduced))
        self.assertFalse(reduced is p)


class TestFactories(unittest.TestCase):

    def test_returns_gen(self):
        p = parallel((d for d in [range(10),range(10)]))
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        def inner_gen():
            for i in range(10):
                yield i
        p = parallel(inner_gen())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(list())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(tuple())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')
        p = parallel(set())
        self.assertTrue(p.__class__.__name__ == 'ParallelGen')

    def test_lazy_parallel(self):
        with self.assertRaises(DeprecationWarning):
            p = lazy_parallel((d for d in [range(10),range(10)]))

    def test_class_depr_warning(self):
        with self.assertRaises(DeprecationWarning):
            p = ParallelSeq((d for d in [range(10),range(10)]))
        with self.assertRaises(DeprecationWarning):
            p = ParallelList((d for d in [range(10),range(10)]))
        with self.assertRaises(DeprecationWarning):
            p = ParallelDict((d for d in [range(10),range(10)]))
        with self.assertRaises(DeprecationWarning):
            p = ParallelString((d for d in [range(10),range(10)]))

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

def chain_iterables(its):
    return chain(*its)

def double(item):
    return item * 2

def double_iterables(l):
    for item in l:
        return map(double, l)

def is_even(i):
    if i & 1:
        return False
    else:
        return True

def double_evens(i):
    if is_even(i):
        return i * 2

def double_evens_in_iterables(l):
    for item in l:
        return map(double, filter(is_even, l))

def double_dict(item):
    k,v = item
    try:
        return [k, [i *2 for i in v]]
    except TypeError:
        return [k, v * 2]

def ret_none(item):
    return None

def is_none_or_false(item):
    return item is None or item is False

if __name__ == '__main__':
    unittest.main()
