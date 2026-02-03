from setuptools import setup, find_packages

setup(
    name="sapi_ingestion_pipeline",
    version="0.0.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
)