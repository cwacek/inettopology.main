
import distribute_setup
distribute_setup.use_setuptools()

from setuptools import setup, find_packages

setup(
    name="inettopology-main",
    version="0.1",
    packages=find_packages(),

    install_requires=[
                      "redis",
                      "argparse"
                     ],

    entry_points={
      'inettopology.modules': [
        'asmap = inettopology.asmap.cmdline'
      ],
      'console_scripts': [
        'inettopology = inettopology:run'
      ]
    },

    author="Chris Wacek",
    author_email="cwacek@cs.georgetown.edu",
    description="Internet Topology Graph Creator",
    license="LGPL"
)
