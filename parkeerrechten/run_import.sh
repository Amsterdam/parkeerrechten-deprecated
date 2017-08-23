#!/bin/bash

set -x
set -e
set -u

# Run our one Go binary (present in the /bin directory).
db2csv -debug \
    -server ljccjhajab.database.windows.net\
    -user usr_0363  \
    -password ${NPR_DATABASE_PASSWORD} \
    -database ODP \
    -query 'select TOP 10 * from VW_0363;'
    -output /tmp/export/npr-data.csv

ls -lh /tmp/export/npr-data.csv


# Include some refactored version of: from parkeerrechten import importer; importer.run_import() (in Make file)


