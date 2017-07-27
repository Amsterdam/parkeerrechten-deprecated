# sqlserver importer

lees data uit MS SQL Server en export naar csv


    docker build  . -t sqlserverimporter
    docker run -v /tmp/export:/tmp/export sqlserverimporter \
        db2csv \
        -debug \
        -server  ${EXTERNAL_PARKEERRECHTEN_DATABASE_HOST}\
        -user  ${EXTERNAL_PARKEERRECHTEN_DATABASE_USER}  \
        -password  ${EXTERNAL_PARKEERRECHTEN_DATABASE_PASSWORD} \
        -database ODP \
       -query "select TOP 10 * from VW_0363;"
