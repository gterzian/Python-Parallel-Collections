from distutils.core import setup
import parallel

setup(
    name='python-parallel-collections',
    data_files=[('', ['requirements.txt', 'README.md', '.gitignore']),],
    version='0.2.2',
    packages=['parallel',],
    description='parallel implementations of collections with support for map/reduce style operations',
    long_description='''
    New in 0.2: a convenient functional interface to the module! 
    
    This package provides a convenient interface to perform map/filter/reduce style operation on standard Python data structures and generators in multiple processes.
    The parallelism is achieved using the Python 2.7 backport of the concurrent.futures package.
    If you can define your problem in terms of map/reduce/filter operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores. 
    Otherwise these datastructures are equivalent to their non-parallel peers found in the standard library.
    \n
    Examples at https://github.com/gterzian/Python-Parallel-Collections
    Feedback and contributions highly sought after!''',
    author='Gregory Terzian',
    author_email='gregory.terzian@gmail.com',
    license='BSD License',
    url='https://github.com/gterzian/Python-Parallel-Collections',
    platforms=["any"],
    requires=['futures',],
    classifiers=[
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: System :: Distributed Computing',
    ],
)