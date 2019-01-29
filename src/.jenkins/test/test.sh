#!/bin/sh

set -e
set -u
set -x

DIR="$(dirname $0)"

dc() {
	docker-compose -p parkeerrechtentest -f ${DIR}/docker-compose.yml $*
}

dc pull
dc build
dc run -u root --rm tests
dc down
