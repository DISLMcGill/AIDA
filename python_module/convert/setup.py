from setuptools import setup
from distutils.core import Extension
import numpy as np
setup(name='convert', version='1.2.1',include_dirs=[np.get_include()], ext_modules=[Extension('convert', ['convert.c'],include_dirs=[np.get_include()])])
