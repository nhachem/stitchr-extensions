#!/usr/bin/python
"""
setup
"""
from setuptools import setup, find_packages

setup(
    name="stitchr_extensions",
    version="0.1",
    description='Stitchr Extensions Python API',
    long_description=' ',# long_description,
    long_description_content_type="text/markdown",
    author='Nabil Hachem',
    author_email='nabilihachem@gmail.com',
    url='https://github.com/nhachem/stitchr-extensions/tree/master/python',
    packages=find_packages(),
    include_package_data=True,
    package_dir={
        'stitchr_extensions.jars': 'deps/jars',
    },
    package_data={
        'stitchr_extensions.jars': ['*.jar'],
        # "": ['*.jar'],
        },
    # scripts=scripts,
    license='http://www.apache.org/licenses/LICENSE-2.0',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Typing :: Typed'],
)
