import os

from setuptools import setup, find_packages

try:
    with open(os.path.abspath('./README.md')) as stream:
        long_description = stream.read()
except:
    long_description = 'pydbwrapper is a simple wrapper for Python psycopg2 with connection pooling'

setup(
    name="pydbwrapper",
    version='1.1.5',
    packages=find_packages(),
    install_requires=['psycopg2-binary', 'DBUtils'],
    classifiers=['Topic :: Database',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 2',
                 'Programming Language :: Python :: 2.6',
                 'Programming Language :: Python :: 2.7',
                 'License :: OSI Approved :: BSD License',
                 'Operating System :: OS Independent',
                 'Intended Audience :: Developers',
                 'Development Status :: 3 - Alpha'],
    author='Alexandre Fidelis',
    author_email='orseni@gmail.com',
    description='pydbwrapper is a simple wrapper for Python psycopg2 with connection pooling',
    long_description=long_description,
    license='BSD',
    keywords='psycopg2 postgresql sql database',
    url='https://github.com/orseni/pydbwrapper',
)
