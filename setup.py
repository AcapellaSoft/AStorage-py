#!/usr/bin/env python

from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(
    name='acapelladb',
    version='0.3.6',
    long_description=long_description,
    long_description_content_type='text/markdown',
    description='Python client for AcapellaDB database',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],
    url='https://github.com/AcapellaSoft/AcapellaDBClient-py',
    author='Acapella Soft',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        'aiohttp == 3.4.4',
        'requests == 2.20.0',
    ],
)
