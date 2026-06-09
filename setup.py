from setuptools import setup, find_packages

setup(
    name="credit_card_defaulter",
    version="0.1.0",
    description="Credit Card Defaulter Analysis — Medallion pipeline utilities",
    packages=find_packages(),
    python_requires=">=3.10",
    install_requires=[
        "pyspark>=3.5.0",
        "pandas>=2.0.0",
        "openpyxl>=3.1.0",
    ],
)
