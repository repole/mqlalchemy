import re
import ast
import os
from setuptools import setup


def read(file_name):
    return open(os.path.join(os.path.dirname(__file__), file_name)).read()

_version_re = re.compile(r'__version__\s+=\s+(.*)')
with open('mqlalchemy/__init__.py', 'rb') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read().decode('utf-8')).group(1)))

setup(
    name='MQLAlchemy',
    version=version,
    url='https://github.com/repole/mqlalchemy',
    license='BSD',
    author='Nicholas Repole',
    author_email='n.repole@gmail.com',
    description='Query SQLAlchemy models with MongoDB syntax.',
    long_description=read('README.md'),
    packages=['mqlalchemy'],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'SQLAlchemy>=0.9'
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent",
    ]
)