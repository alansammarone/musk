import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="musk",
    version="0.0.1",
    author="Alan Sammarone",
    author_email="alansammarone@gmail.com",
    description="A simulations package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/alansammarone/musk",
    packages=["musk"],
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.6",
    install_requires=[
        "Pillow",
        "matplotlib",
        "seaborn",
        "scipy",
        "environs",
        "mysql-connector-python",
        "pydantic",
        "boto3",
    ],
)
