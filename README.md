# Deirokay
[![build](https://img.shields.io/github/workflow/status/bigdatabr/deirokay/Test)](https://github.com/bigdatabr/deirokay/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/bigdatabr/deirokay/branch/master/graph/badge.svg?token=Fee3QNLC2s)](https://codecov.io/gh/bigdatabr/deirokay)
[![docstr_coverage](https://img.shields.io/endpoint?url=https://jsonbin.org/bressanmarcos/deirokay/badges/docstr-cov)](https://github.com/HunterMcGushion/docstr_coverage)
[![license: MIT](https://img.shields.io/github/license/bigdatabr/deirokay)](https://github.com/bigdatabr/deirokay/blob/master/LICENSE)
[![code style: flake8](https://img.shields.io/badge/code%20style-flake8-000000.svg)](https://flake8.pycqa.org/en/latest/internal/writing-code.html)
[![docstring: numpy](https://img.shields.io/badge/docstring-numpy-008080.svg)](https://numpydoc.readthedocs.io/en/latest/format.html)
[![imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![semantic-release: conventionalcommits](https://img.shields.io/badge/semantic--release-conventionalcommits-e10079?logo=semantic-release)](https://www.conventionalcommits.org/)


Deirokay (*dejɾo'kaj*) is a tool for data profiling and data validation.

Deirokay separates document parsing from validation logic,
so that you can create your statements about your data
without worrying whether or not your file has been properly
parsed.

You can use Deirokay for:
- Data parsing from files (CSV, parquet, excel, or any other
pandas-compatible format);
- Data validation, via *Deirokay Statements*;
- Data profiling, which generates Deirokay Statements automatically
based on an existing file. You may use these statements later against
new documents to make sure the validation still holds for new data.

## Installation

To start using Deirokay, install its package by following the instructions below.

By default, only core dependencies are installed when supplying the pip
command:

```bash
pip install Deirokay
```

Depending on your use cases, you need to include extra dependencies.
Use any of the commands below:

```bash
pip install Deirokay[s3]  # Optional dependencies for use with AWS S3
pip install Deirokay[pandas]  # For Pandas backend
pip install Deirokay[dask]  # For Dask or Dask Distributed backends
```

You may also install any combination of the extras above by separating
them with commas:

```bash
pip install Deirokay[dask,s3]  # Dask + S3 deps
```

To check if Deirokay is properly installed and to know its version,
enter the following command in the Python prompt:

```
>>> import deirokay
>>> deirokay.__version__
'X.Y.Z'
```

If you wish to contribute for Deirokay development, you need to install
a more restrict set of packages, in order to guarantee that you are
always creating and testing code to work with least recent supported
versions:

```bash
pip install Deirokay[dev]
```

## Documentation

Please, [read the docs](https://deirokay.readthedocs.io/).

## Contributing

Check our [contributing](./CONTRIBUTING.md) guidelines.
