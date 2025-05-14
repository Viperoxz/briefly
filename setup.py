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
        "readability-lxml",
        "tenacity",
        "pymongo",
        "tenacity",
        "sentence-transformers",
        "groq",
        "openai",
        "pymongo",
        "langchain",
        "pyvi",
        "langchain-groq",
        "langchain-core",
        "uuid",
        "qdrant-client",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
