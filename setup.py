from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))


setup(
    name='ArangoFlow',

    version='0.01',

    description='A python package for managing experiments with ArangoDB',
    long_description='',

    url='',

    author='Tariq Daouda',
    author_email='You can figure it out with google',

    license='ApacheV2',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
    ],

    install_requires=['grequests', "pyArango"],

    keywords='',

    packages=find_packages(),

    entry_points={
        'console_scripts': [
            'sample=sample:main',
        ],
    },
)
