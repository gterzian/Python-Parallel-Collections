from distutils.core import setup
import parallel

setup(
    name='python-parallel-collections',
    data_files=[('', ['requirements.txt', 'README.md', '.gitignore']),],
    version='2.0.0',
    packages=['parallel',],
    description='parallel support for map/reduce style operations',
    long_description='''

    This package provides a convenient interface to perform map/filter/reduce style operation on standard Python data structures and generators in multiple processes.
    The parallelism is achieved using the Python 2.7 backport of the concurrent.futures package.
    If you can define your problem in terms of map/reduce/filter operations, it will run on several parallel Python processes on your machine, taking advantage of multiple cores.
    \n
    Examples at https://github.com/gterzian/Python-Parallel-Collections
    Feedback and contributions highly sought after!''',
    author='Gregory Terzian',
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
