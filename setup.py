#!/usr/bin/env python

from distutils.core import setup

setup(name='astorage',
      version='0.1.0',
      description='Python client for AStorage database',
      url='https://srv.nppsatek.ru:42917/hiload/AStorageClient-py',
      package_dir={'': 'src'},
      packages=['acapella.kv'],
)
