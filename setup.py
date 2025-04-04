from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="databricks-maintenance-toolkit",
    version="0.1.0",
    author="Sunny Sharma",
    author_email="sunny.sharma@datalytyx.com",
    description="A toolkit to automate maintenance tasks for Databricks environments",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/username/databricks-maintenance-toolkit",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "requests>=2.27.0",
        "beautifulsoup4>=4.10.0",
        "pandas>=1.3.0",
        "python-dateutil>=2.8.2",
        "packaging>=21.0",
        "pyyaml>=6.0",
        "click>=8.0.0",
        "tabulate>=0.8.9",
    ],
    entry_points={
        "console_scripts": [
            "databricks-maintenance=databricks_maintenance.cli:main",
        ],
    },
)
