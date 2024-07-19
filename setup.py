import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'Scripts',
    version = '0.1',
    author = 'Zach Boquet',
    author_email = 'zach.boquet@gmail.com',
    description = 'Scripts needed for tutorial notebooks to work.',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/Kurokio/HDRL-Internship-2024',
    packages = setuptools.find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "License :: OSI Approved",
    ],
    license = 'Apache-2.0'
)