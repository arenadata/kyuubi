#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="kyuubi-spark-connect",
    version="1.0.2",
    description="Kyuubi Spark Connect Python client",
    long_description=(
        "Python client for connecting to Apache Spark via Kyuubi Spark Connect frontend. "
        "Drop-in replacement for PySpark SparkSession.builder with Kerberos/LDAP auth and "
        "transparent ZooKeeper-based failover."
    ),
    packages=["kyuubi"],
    package_dir={"": "."},
    install_requires=[
        "grpcio>=1.60.1",
        "protobuf>=4.25.2",
        "kazoo>=2.8",
    ],
    extras_require={
        "kerberos": ["gssapi>=1.11.1"],
    },
)
