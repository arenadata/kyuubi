#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="kyuubi-spark-connect",
    version="1.0.0",
    description="Kyuubi Spark Connect Python client",
    packages=["kyuubi"],
    package_dir={"": "."},
    install_requires=[
        "grpcio>=1.60.1",
        "protobuf>=4.25.2",
    ],
    extras_require={
        "kerberos": ["gssapi>=1.11.1"],
    },
)
