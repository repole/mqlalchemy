import re
import ast
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
    license='MIT',
    author='Nicholas Repole',
    author_email='n.repole@gmail.com',
    description='Query SQLAlchemy models with MongoDB syntax.',
    packages=['mqlalchemy', 'mqlalchemy.tests'],
    zip_safe=False,
    platforms='any',
    test_suite='mqlalchemy.tests',
    tests_require=[
        'SQLAlchemy>=0.9'
    ],
    install_requires=[
        'SQLAlchemy>=0.9'
    ],
    keywords=['mongodb', 'sqlalchemy', 'json', 'sql'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database :: Front-Ends",
        "Operating System :: OS Independent"
    ]
)