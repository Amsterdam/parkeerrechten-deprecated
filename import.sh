#!/bin/sh

set -e
set -u

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
docker-compose -p parkeerrechten run --rm sqlserverimporter db2csv \
        -debug \
        -server ljccjhajab.database.windows.net\
        -user usr_0363  \
        -password ${NPR_DATABASE_PASSWORD} \
        -database ODP \
        -query 'select TOP 10 * from VW_0363;'

# Load data from objectstore 2016 (raw dumps)
#dc run --rm importer

#dc run --rm db-backup
