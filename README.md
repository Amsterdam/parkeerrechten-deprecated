# parkeerrechten

## Development

    # Lookup the objectstore password in Rattic
    export PARKEREN_OBJECTSTORE_PASSWORD=xxxx

    # start database
    docker-compose up -d database

    # run import - option 1
    docker-compose build && docker-compose run app make runimport

    # run import - option 2
    # handy during development, allows you to edit the import scripts
    # and run again without restarting the container..
    docker-compose run -v "$PWD"/parkeerrechten:/app/parkeerrechten app bash
    make runimport


    # cleanup
    docker-compose down