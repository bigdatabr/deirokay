Installation
============

To start using Deirokay, install its package by following the instructions below.

By default, only core dependencies are installed when supplying the pip
command:

.. code-block:: bash

    pip install Deirokay

Depending on your use cases, you need to include extra dependencies.
Use any of the commands below:

.. code-block:: bash

    pip install Deirokay[s3]  # Optional dependencies for use with AWS S3
    pip install Deirokay[pandas]  # For Pandas backend
    pip install Deirokay[dask]  # For Dask or Dask Distributed backends

You may also install any combination of the extras above by separating
them with commas:

.. code-block:: bash

    pip install Deirokay[dask,s3]  # Dask + S3 deps


If you wish to contribute for Deirokay development, you need to install
a more restrict set of packages, in order to guarantee that you are
always creating and testing code to work with least recent supported
versions:

.. code-block:: bash

    pip install Deirokay[dev]
