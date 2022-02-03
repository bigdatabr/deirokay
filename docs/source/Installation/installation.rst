============
Installation
============
Before you can use Deirokay, you will need install it. This guide will 
show all possibilities that you have to install Deirokay. But first you 
will need to get python and pip ready to go on your machine.

EXEMPLO -> instalacao python e pip

To start using Deirokay, install its package and follow
the instructions bellow.

This page guides you throught the installation process. And you can get 

Install Deirokay directly from master branch typing in your
command line:
pip install git+http://gitlab.bigdata/data-engineers/deirokay

To include optional dependences for AWS S3, install:
pip install git+http://gitlab.bigdata/data-engineers/deirokay[s3]

If you want to be in sync with the latest (and possibly unstable) 
release: 
pip install git+http://gitlab.bigdata/data-engineers/deirokay@dev



Installation for development
============================

If you wish to contribute for Deirokay development, maybe
you will want to install a more complete set of packages for
testing and to help you in your development.
pip install git+http://gitlab.bigdata/data-engineers/deirokay[dev]
