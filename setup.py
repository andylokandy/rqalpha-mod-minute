from pip._internal.req import parse_requirements

from setuptools import (
    find_packages,
    setup,
)

setup(
    name='rqalpha-mod-minute',
    version="0.1.0",
    packages=find_packages(exclude=[]),
    license='Apache License v2',
    package_data={'': ['*.*']},
    install_requires=[str(ir.req) for ir in parse_requirements("requirements.txt", session=False)],
    zip_safe=False,
)