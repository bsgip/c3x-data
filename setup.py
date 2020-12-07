import setuptools
import sys

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    'pandas',
    'numpy',
    'sqlalchemy'
]

setuptools.setup(
    name="c3x-data",
    version="0.0.1",
    author="BSGIP",
    description="For data analysis and preparation",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url=None,
    packages=setuptools.find_packages(),
    classifiers=[
    ],
    install_requires=install_requires,
    python_requires='>=3.6',
    license='MIT'
)
