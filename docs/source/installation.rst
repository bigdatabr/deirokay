Installation
============

Before you can use Deirokay, you will need install it. This guide will 
show all possibilities that you have to install Deirokay. But first you 
will need to get python and pip ready to go on your machine, in case that
you don't have yet.

.. code-block:: bash

    sudo apt install python3 python3-pip

To start using Deirokay, install its package and follow
the instructions bellow.

This page guides you through the installation process. And you can get 

Install Deirokay directly from master branch typing in your
command line:

.. code-block:: bash

    pip install git+http://gitlab.bigdata/data-engineers/deirokay

To include optional dependencies for AWS S3, install:

.. code-block:: bash

    pip install git+http://gitlab.bigdata/data-engineers/deirokay[s3]


If you wish to contribute for Deirokay development, maybe
you will want to install a more complete set of packages for
testing to help you in your development.

.. code-block:: bash

    pip install git+http://gitlab.bigdata/data-engineers/deirokay[dev]
