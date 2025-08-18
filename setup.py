# setup.py
# This file allows pip to install the local fabric_cicd package

from setuptools import setup, find_packages

setup(
    name="fabric-cicd",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "azure-identity>=1.14.0",
        "pyyaml>=6.0.2",
        "requests>=2.32.0"
    ],
    author="Muhammad",
    description="Local deployment tool for Microsoft Fabric CI/CD",
)
