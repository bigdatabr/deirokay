from setuptools import find_packages, setup

from deirokay.__version__ import __version__

with open('README.md') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    requirements = [line.strip() for line in f.readlines()]

with open('requirements-dev.txt') as f:
    requirements_dev = [line.strip() for line in f.readlines()]

setup(
    name="deirokay",
    packages=find_packages(include='deirokay*'),
    version=__version__,
    author="Marcos Bressan",
    author_email="marcos.bressan@bigdata.com.br",
    description="A tool for data profiling and data validation",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="http://gitlab.bigdata/bressanmarcos/deirokay",
    classifiers=[
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    include_package_data=True,
    zip_safe=True,
    install_requires=requirements,
    extras_require={
        'dev': requirements_dev
    }
)
