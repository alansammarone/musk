import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="musk",  # Replace with your own username
    version="0.0.1",
    author="Alan Sammarone",
    author_email="alansammarone@gmail.com",
    description="A simulations package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.7",
)
