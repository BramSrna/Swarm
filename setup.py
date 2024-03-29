#!/usr/bin/env python3

import os
from setuptools import setup

directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='NetworkManager',
      version='1.0.0',
      description='Extensible utility for deploying and controlling a network of nodes.',
      author='Abraham Srna',
      license='MIT',
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=['network_manager', 'swarm'],
      classifiers=[
          "Programming Language :: Python :: 3",
          "License :: OSI Approved :: MIT License"
      ],
      install_requires=['logging'],
      python_requires='>=3.10',
      extras_require={
          "testing": [
              "pytest",
          ]
      },
      include_package_data=True)
