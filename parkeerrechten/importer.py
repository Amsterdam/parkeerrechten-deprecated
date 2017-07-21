"""Import shape file. Fill the database.

"""

from parkeerrechten import objectstore, settings
import subprocess
import psycopg2
import zipfile
import os

container = 'parkeerrechten'
download_dir = '/tmp/parkeerrechten/'
extract_dir = '/tmp/parkeerrechten/unzip'


def run_import():
    create_table()
    for file in objectstore.fetch_import_file_names(container):
        retrieve_from_objectstore_and_import(file)
    check_import()


def retrieve_from_objectstore_and_import(filename):
    file = objectstore.copy_file_from_objectstore(
        container=container,
        download_dir=download_dir,
        file_name=filename)
    zip_ref = zipfile.ZipFile(file, 'r')
    zip_ref.extractall(extract_dir)
    zip_ref.close()
    for f in os.listdir(extract_dir):
        if f.endswith('.csv'):
            print('Importing: ', f)
            import_csv(f)


def import_csv(file):
    print('Import csv ', file)
    conn = psycopg2.connect(settings.PG_LOGIN)
    cur = conn.cursor()
    full_name = extract_dir + '/' + file
    try:
        f = open(full_name, encoding='latin-1')
        cur.copy_from(f, 'parkeerrecht_2016_raw',  sep=";")
        conn.commit()
        os.remove(full_name)
        print('Import csv successfull')
    except Exception as e:
        conn.rollback()
        print('Error during create view. ', e)
        raise e
    finally:
        cur.close()
        conn.close()


def create_table():
    print('Create table')
    conn = psycopg2.connect(settings.PG_LOGIN)
    cur = conn.cursor()
    try:
        cur.execute("""DROP TABLE IF EXISTS parkeerrecht_2016_raw;""")
        cur.execute("""
        CREATE TABLE parkeerrecht_2016_raw(
            JAAR varchar(255),
            PARKEERRECHT_ID	varchar(255),
            DATUM	varchar(255),
            MAAND	varchar(255),
            RECHT_BEGINTIJD varchar(255),
            RECHT_EINDTIJD	varchar(255),
            BUURT_CODE varchar(255),
            BUURT varchar(255),
            BRTCOMBINATIE_CODE varchar(255),
            BRTCOMBINATIE varchar(255),
            STADSDEEL_CODE varchar(255),
            STADSDEEL varchar(255),
            LAND_CODE varchar(255),
            LAND varchar(255),
            RECHTTYPE_CODE varchar(255),
            RECHTTYPE varchar(255),
            GEBRUIKSDOEL_CODE varchar(255),
            GEBRUIKSDOEL varchar(255),
            GEBIED_CODE varchar(255),
            GEBIED varchar(255),
            GEBIEDTYPE_CODE varchar(255),
            GEBIEDTYPE varchar(255),
            IND_FISCAAL varchar(30),
            HOOFDGROEP varchar(255),
            POSTCODE6 varchar(255),
            POSTCODE4 varchar(255),
            REGISTRATIETIJD varchar(255),
            REG_EINDTIJD_RECHT varchar(255),
            AANGEPASTE_EINDTIJD varchar(255),
            VERKOOPKANAAL_CODE varchar(255),
            VERKOOPPUNTBRT_CODE varchar(255),
            VERKOOPPUNTBRT varchar(255),
            VERKOOPPUNTBRTCOMB_CODE varchar(255),
            VERKOOPPUNTBRTCOMB varchar(255),
            VERKOOPPUNTSTDSDEEL_CODE varchar(255),
            VERKOOPPUNTSTADSDEEL varchar(255),
            VERKOOPPUNTCODE varchar(255),
            VERKOOPPUNT varchar(255),
            VERKOOPPUNTPOSTCODE4 varchar(255),
            VERKOOPKANAAL varchar(255),
            IND_WINKELSTRAAT varchar(255),
            VER_BATCH_ID varchar(255),
            VER_BATCH_NAAM varchar(255),
            STADSDEELCODE_VERG_TARIEFGEB varchar(255),
            STADSDEEL_VERG_TARIEFGEB varchar(255)
        )""")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print('Error during create table. ', e)
        raise e
    finally:
        cur.close()
        conn.close()
        print('Create table successfull')


def check_import():
    print('Check import')
    conn = psycopg2.connect(settings.PG_LOGIN)
    cur = conn.cursor()

    cur.execute('SELECT count(*) FROM "parkeerrecht_2016_raw";')
    result = cur.fetchone()
    million = 1000000
    if result[0] < 20 * million:
        raise Exception("Too little records in database, import failed.")

    cur.close()
    conn.close()
    print('Import successfull')


