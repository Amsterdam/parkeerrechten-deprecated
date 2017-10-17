# parkeerrechten

* Code to import Parkeerrechten data from NPR and make it available as a database.
* To run use docker-compose on the provided image, this will run the full import
  pipeline.
* Note you need the NPR database password and the object store password for
  parkeren in your environment (see the docker-compose.yml file for more).


### Considerations / design decisions
* We cannot backup the full NPR database overnight
* Batches that are fully processed are labeled by the day they were processed.
  Our code uses this labeling to determine which data to back up. (The labels
  are in the "VER_BATCH_NAAM" field.)
* Current design is to use the object store for backups and keeping state.

The backup scripts do the following:
* The backup scripts check the named database dumps on the object store to
  determine which data to back up next. The naming of the database uses the
  same date labels as the NPR database uses for the batches. The names are
  extracted from the filenames. They are either 'Leeg' or dates in the
  YYYYMMDD layout.
* The stored dates (extracted from the backup file names) are sorted and
  compared to what is available from the NPR database. The dates that still
  have to be downloaded are sorted and only the oldest 10 are downloaded.
* The downloading moves data from the MSSQL server at NPR to the locally
  running PostgreSQL server (the latter is only active during the backup
  operation).
* After running the script that transfers data to our local PostgreSQL
  database another Python script queries the local database to find out
  which batches it contains (again by checking the dates). These are then
  dumped and uploaded to the datastore (if they were not already on the
  object store).
* These scripts run daily, and will overtake the data available from the
  NPR at a rate of 10 days per import run. If there is no data to backup
  the scripts are smart enough to just give up (no configuration needed).

### Running
We provide a docker-comopse file that allows you to test the software without
having access to the real data sources and back-up systems.

```shell
docker-compose -f src/.jenkins/test/docker-compose.yml run tests
```

To run the software in production you will need passwords to the NPR
database and the object store. See the `docker-compose.yml` in the
project root.


