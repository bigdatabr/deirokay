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

Install Deirokay using pip:

`pip install Deirokay`

To include optional dependences for AWS S3, install:

`pip install Deirokay[s3]`

### Installation for development

If you wish to contribute for Deirokay development, maybe
you will want to install a more complete set of packages for
testing and to help you in your development.

`pip install Deirokay[dev]`

## Documentation

Please, [read the docs](https://deirokay.readthedocs.io/).

## Instructions for testing

Make sure you set the `DEIROKAY_TEST_BUCKET` environment variable to a
bucket that you have access to (*s3:GetObject*, *s3:PutObject* and 
*s3:DeleteObject* actions), otherwise some tests that depends on AWS S3
will be skipped.
