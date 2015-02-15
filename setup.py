import re
import ast
import os
from setuptools import setup

_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('mqlalchemy/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name='MQLAlchemy',
    version=version,
    url='https://github.com/repole/mqlalchemy',
    download_url="https://github.com/repole/mqlalchemy/tarball/" + version,
    license='BSD',
    author='Nicholas Repole',
    author_email='n.repole@gmail.com',
    description='Query SQLAlchemy models with MongoDB syntax.',
    packages=['mqlalchemy'],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'SQLAlchemy>=0.9'
    ],
    keywords=['mongodb', 'sqlalchemy', 'json', 'sql'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ]
)