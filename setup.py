from distutils.core import setup
from setuptools import find_packages

setup(
    name="PyMDDT",
    version="0.0.1",
    description="Python MultiModal Data Toolkit",
    author="Eduardo Davalos Anaya",
    author_email="eduardo.davalos.anaya@vanderbilt.edu",
    packages=["mm"],
    install_requires=["pathlib", "networkx"]
)
