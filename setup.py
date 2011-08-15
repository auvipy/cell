#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import codecs

extra = {}
tests_require = ["nose", "nose-cover3"]
if sys.version_info >= (3, 0):
    extra.update(use_2to3=True)
elif sys.version_info <= (2, 6):
    tests_require.append("unittest2")
elif sys.version_info <= (2, 5):
    tests_require.append("simplejson")


if sys.version_info < (2, 6):
    raise Exception("cl requires Python 2.6 or higher.")

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup  # noqa

from distutils.command.install import INSTALL_SCHEMES

os.environ["CL_NO_EVAL"] = "yes"
import cl
os.environ.pop("CL_NO_EVAL", None)
sys.modules.pop("cl", None)

packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
    os.chdir(root_dir)
src_dir = "cl"


def fullsplit(path, result=None):
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)


for scheme in list(INSTALL_SCHEMES.values()):
    scheme['data'] = scheme['purelib']

for dirpath, dirnames, filenames in os.walk(src_dir):
    # Ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith("."):
            del dirnames[i]
    for filename in filenames:
        if filename.endswith(".py"):
            packages.append('.'.join(fullsplit(dirpath)))
        else:
            data_files.append([dirpath, [os.path.join(dirpath, f) for f in
                filenames]])

if os.path.exists("README.rst"):
    long_description = codecs.open('README.rst', "r", "utf-8").read()
else:
    long_description = "See http://pypi.python.org/pypi/cl"

setup(
    name='cl',
    version=cl.__version__,
    description=cl.__doc__,
    author=cl.__author__,
    author_email=cl.__contact__,
    url=cl.__homepage__,
    platforms=["any"],
    packages=packages,
    data_files=data_files,
    zip_safe=False,
    test_suite="nose.collector",
    install_requires=[
        'kombu>=1.2.1',
    ],
    tests_require=tests_require,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 2.6",
        "Intended Audience :: Developers",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points={
        "console_scripts": ["cl = cl.bin.cl:main"],
    },
    long_description=long_description,
    **extra)
