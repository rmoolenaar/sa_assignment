#!/bin/sh

rm -Rf target
tar czf docker/project.tgz .

docker build -t spark docker

docker image ls

rm docker/project.tgz