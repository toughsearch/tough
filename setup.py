from distutils.core import setup

from Cython.Build import cythonize

setup(name='Tough Search', ext_modules=cythonize("tough/dt/dt.pyx"))
