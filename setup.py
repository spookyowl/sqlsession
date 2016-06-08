#!/usr/bin/env python
from setuptools import setup

setup(
    name="sqlsession",
    version="0.1",
    description='',
    author='Peter Facka',
    author_email='pfacka@hexcells.com',
    packages=[
        'sqlsession',
    ],
    zip_safe=False,	
    install_requires=[
        'SQLAlchemy>=1.0.6',
        'psycopg2==2.6.1'
    ],
    provides=['sqlsession (0.1)'],
    include_package_data=True,
)
