import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="adam-squared-pypes",
    version="0.1.0dev3",
    author="Adam Beecham",
    author_email="adam.beecham@adamsquared.cloud",
    description="A framework for building lightweight, stateless data pipelines with python",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/adam-squared/pypes",
    packages=setuptools.find_packages(),
    python_requires=">=3.7",
)
