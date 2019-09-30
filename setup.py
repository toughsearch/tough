from setuptools import find_packages, setup

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["indexed-gzip~=0.8.10", "pyyaml~=5.1.1", "tqdm~=4.31.1"]

setup(
    name="toughsearch",
    version="0.1.0",
    author="Dmitriy Doroshev",
    author_email="r.schweppes@ya.ru",
    description="Simple date-based search engine for text- and gzip-logs",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/toughsearch/tough",
    packages=find_packages(),
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
)
