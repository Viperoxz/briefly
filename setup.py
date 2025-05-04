#setup.py
from setuptools import find_packages, setup

setup(
    name="news_pipeline",
    packages=find_packages(exclude=["news_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "pandas",
        "pymongo",
        "feedparser",
        "beautifulsoup4",
        "requests",
        "lxml",
        "python-dotenv",
        "pydantic",
        "groq",
        "tenacity"

    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
