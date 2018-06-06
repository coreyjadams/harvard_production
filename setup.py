import os
from setuptools import setup, find_packages

here = os.path.dirname(__file__)

def read(fname):
    return open(os.path.join(here, fname)).read()


requirements = [
    'PyYAML',
    'mysqlclient',
]

test_requirements = [
    'pytest>=3.0.0',
    'pytest-mysql',
]

extras_require = {
    'test' : test_requirements
}

setup(
    name = "production-tools",
    version = "0.0.1",
    author = "Corey Adams",
    author_email = "coreyjadams@gmail.com",
    description = ("A set of tools for running and managing large scale file productions."),
    license = "BSD",
    keywords = "database production slurm grid",
    url = "https://github.com:Harvard-Production/production-tools",
    package_dir={'': 'src'},
    packages=find_packages('src'),
    long_description=read('README.md'),
    install_requires=requirements,
    tests_require=test_requirements,
    test_suite='tests',
    include_package_data=True,
    zip_safe=False,
    extras_require=extras_require,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: BSD License",
    ],
)