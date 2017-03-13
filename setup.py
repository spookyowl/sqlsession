#!/usr/bin/env python

from setuptools import setup

setup(
    name="sqlsession",
    version="0.1.9",
    description='Dirt simple CRUD API to access SQL databases',
    author='Peter Facka',
    url='https://bitbucket.org/trackingwire/sqlsession',
    author_email='pfacka@trackingwire.com',
    license='MIT Licence (http://opensource.org/licenses/MIT)',
    packages=[
        'sqlsession',
    ],
    zip_safe=False,	
    install_requires=[
        'SQLAlchemy>=1.0.6',
        'psycopg2>=2.6.1'
    ],
    provides=['sqlsession (0.1.9)'],
    include_package_data=True,
    classifiers=[
      'Development Status :: 3 - Alpha',
      'Environment :: Web Environment',
      'Intended Audience :: Developers',
      'Operating System :: Microsoft :: Windows',
      'Operating System :: MacOS :: MacOS X',
      'Operating System :: POSIX',
      'Programming Language :: Python :: 2.7',
      'License :: OSI Approved :: MIT License',
      ],
)
