from distutils.core import setup, Extension
import numpy as np
setup(name='convert', version='0.0.1',include_dirs=[np.get_include()], ext_modules=[Extension('convert', ['convert.c'])])
