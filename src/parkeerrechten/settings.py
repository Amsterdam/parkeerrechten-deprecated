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
LOCAL_TABLE = 'VW_0363_TEMPORARY'
TARGET_TABLE = 'BACKUP_VW_0363'

SENSITIVE_FIELDS = ['KENM_RECHTV_INT']

# downstream object store
_PARKEREN_OBJECTSTORE_PASSWORD = os.environ['PARKEREN_OBJECTSTORE_PASSWORD']

OBJECTSTORE_CONFIG = {
    'user': 'parkeren',
    'key': _PARKEREN_OBJECTSTORE_PASSWORD,
    'tenant_name': 'BGE000081_Parkeren',
    'tenant_id': 'fd380ccb48444960837008800a453122',
}

OBJECT_STORE_CONTAINER = 'parkeerrechten_pgdumps'
BACKUP_FILE_BASENAME = os.environ['BACKUP_FILE_BASENAME']

# temporary database used during import (will be pg_dumped and uploaded)
DATAPUNT_DB_URL = URL(
    drivername='postgresql',
    username='parkeerrechten',
    password='insecure',
    host='database',
    database='parkeerrechten',
)

DATAPUNT_TEST_DB_URL = URL(
    drivername='postgresql',
    username='parkeerrechten',
    password='insecure',
    host='nprstandin',
    database='parkeerrechten',
)

N_DAYS_PER_RUN = int(os.environ['BACKUP_N_DAYS_PER_RUN'])

DEBUG = True if os.environ.get('DEBUGRUN', '') == 'TRUE' else False

BATCH_SIZE = int(os.environ['BACKUP_BATCH_SIZE']) if not DEBUG else 10
BASENAME = os.environ['BACKUP_FILE_BASENAME']
