#!/usr/bin/env python

from distutils.core import setup

setup(name='acapelladb',
      version='0.3.2',
      description='Python client for AcapellaDB database',
      url='https://srv.nppsatek.ru:42917/hiload/AStorageClient-py',
      package_dir={'': 'src'},
      packages=['acapella.kv'],
)
