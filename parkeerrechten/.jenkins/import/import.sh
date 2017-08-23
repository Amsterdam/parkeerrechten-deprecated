#!/bin/sh

set -e
set -u
set -x

DIR="$(dirname $0)"

dc() {
	docker-compose -p parkeerrechten -f ${DIR}/docker-compose.yml $*
}

trap 'dc kill ; dc rm -f' EXIT

rm -rf ${DIR}/backups
mkdir -p ${DIR}/backups


dc up -d --build database
sleep 20

# Load data from NPR Database, save in /tmp/parkeererechten/export/output.csv
dc run --rm importer

# dc run --rm db-backup   # TBD
