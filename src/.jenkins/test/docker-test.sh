#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Running tests for parkeerrechten docker container"
cd ../../ && make flake test
