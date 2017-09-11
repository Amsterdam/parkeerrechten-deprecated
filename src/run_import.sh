#!/bin/bash

set -x
set -e
set -u

# Run our one Go binary (present in the /bin directory).
ls -lh /tmp/export
whoami
touch /tmp/export/text.txt

db2csv -debug \
    -server ljccjhajab.database.windows.net\
    -user usr_0363  \
    -password ${NPR_DATABASE_PASSWORD} \
    -database ODP \
    -query 'select TOP 10 * from VW_0363;' \
    -output /tmp/export/npr-data.csv

ls -lh /tmp/export/npr-data.csv
echo "Number of lines in CSV"
wc -l /tmp/export/npr-data.csv

python -c 'from parkeerrechten import importer; importer.run_import()'

chmod -R a+w /tmp/parkeerrechten/
chmod -R a+w /tmp/export/


# Include some refactored version of: from parkeerrechten import importer; importer.run_import() (in Make file)


