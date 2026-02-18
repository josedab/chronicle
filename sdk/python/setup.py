"""Setup for Chronicle Python SDK."""
from setuptools import setup, find_packages

setup(
    name="chronicle-db",
    version="0.5.0",
    description="Python SDK for Chronicle Time-Series Database",
    packages=find_packages(),
    python_requires=">=3.8",
    extras_require={
        "pandas": ["pandas>=1.0"],
    },
)
