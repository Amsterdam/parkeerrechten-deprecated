import os

from sqlalchemy.engine.url import URL

# NPR upstream database with parkeerrechten
_NPR_DATABASE_PASSWORD = os.environ['NPR_DATABASE_PASSWORD']

NPR_DB_URL = URL(
    drivername='mssql+pymssql',
    username='usr_0363@ljccjhajab',
    password=_NPR_DATABASE_PASSWORD,
    host='ljccjhajab.database.windows.net',
    port=1433,
    database='ODP',
)
NPR_TABLE = 'VW_0363'

# downstream object store
_PARKEREN_OBJECTSTORE_PASSWORD = os.environ['PARKEREN_OBJECTSTORE_PASSWORD']

OBJECTSTORE_CONFIG = {
    'user': 'parkeren',
    'key': _PARKEREN_OBJECTSTORE_PASSWORD,
    'tenant_name': 'BGE000081_Parkeren',
    'tenant_id': 'fd380ccb48444960837008800a453122',
}

# temporary database used during import (will be pg_dumped and uploaded)
DATAPUNT_DB_URL = URL(
    drivername='postgresql',
    username='parkeerrechten',
    password='insecure',
    host='database',
    database='parkeerrechten',
)

