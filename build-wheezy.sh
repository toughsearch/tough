#!/usr/bin/env sh

set -ex

docker build -t python:3.6-wheezy -f docker-python/Dockerfile.python36.wheezy .

docker build -t tough-build-wheezy -f Dockerfile.wheezy .

docker run -it --rm -v `pwd`:/app tough-build-wheezy \
    pyinstaller -F -n tough --distpath build-wheezy/dist --workpath build-wheezy/build -y \
    tough/__main__.py
