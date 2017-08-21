#!/usr/bin/env python

from distutils.core import setup

setup(name='astorage',
      version='0.1.0',
      description='Python client for AStorage database',
      url='https://srv.nppsatek.ru:42917/hiload/AStorageClient',
      package_dir={'': 'python/src'},
      packages=['acapella.kv'],
)
