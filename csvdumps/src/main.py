import logging
import os
import re
import shlex
import subprocess
import sys
import time
from subprocess import PIPE, Popen
from typing import NamedTuple

import objectstore
import psycopg2

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logging.basicConfig(
    stream=sys.stdout,
    level=logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


class Config(NamedTuple):
    output_dir: str
    container: str
    database_host: str
    database_name: str
    database_user: str
    database_password: str


class Exporter:
    def __init__(self):
        self.config = Config(
            output_dir=os.getenv("OUTPUT_DIR", "/data"),
            container=os.getenv("OBJECTSTORE_CONTAINER"),
            database_host=os.getenv("DATABASE_HOST"),
            database_name=os.getenv("DATABASE_NAME"),
            database_user=os.getenv("DATABASE_USER"),
            database_password=os.getenv("DATABASE_PASSWORD"),
        )
        logging.debug(f"Config: {self.config}")

        # Setup the connection to the objectstore
        self.objectstore_conn = objectstore.get_connection()

        self.setup_database_connection()

    def setup_database_connection(self):
        # Setup the database connection
        retries = 5
        while retries > 0:
            try:
                logger.warning("Setting up connection to the database")
                self.db_conn = psycopg2.connect(
                    host=self.config.database_host,
                    database=self.config.database_name,
                    user=self.config.database_user,
                    password=self.config.database_password,
                )
                self.db_cur = self.db_conn.cursor()
                return
            except psycopg2.OperationalError:
                if retries <= 0:
                    raise
                retries -= 1
                logger.warning("Retrying connection to the database")
                time.sleep(1)

    def list_files(self, container, pattern=None):
        """
        List files from a container
        If a pattern has been given it will only return messages matching this pattern
        """
        files = objectstore.get_full_container_list(self.objectstore_conn, container)
        if pattern:
            return [file for file in files if re.match(pattern, file["name"])]
        return files

    def download_file(self, container, file, destination=None, overwrite=False):
        if not destination:
            destination = file["name"]

        path = self.ensure_path(destination)

        if os.path.exists(path) and not overwrite:
            logger.info(f"Skipping file: {destination} since it exists")
            return

        size_mb = round(file["bytes"] / 1024 / 1024, 2)
        logger.info(f"Downloading {file} - {size_mb} MB")
        headers, body = self.objectstore_conn.get_object(container, file["name"])
        with open(path, "wb") as f:
            logger.info(f"Writing to {path}")
            f.write(body)

        return path

    def restore_table(self, file):
        if not isinstance(file, str):
            raise Exception("file must be the path to the pg_dump file")

        command = [
            "pg_restore",
            "--clean",
            "-h",
            self.config.database_host,
            "-d",
            self.config.database_name,
            "-U",
            self.config.database_user,
            "--no-password",
            file,
        ]

        logger.info(f"Restoring {file} into the database")
        try:
            output = subprocess.run(
                command,
                capture_output=True,
                check=False,
                env={"PGPASSWORD": self.config.database_password},
            )
        except subprocess.CalledProcessError as e:
            logger.error(e.stderr)
            logger.exception(e)
            raise e

        logger.debug(f"pg_restore output: {output}")
        return output

    def dump_table_csv(self, table, file):
        sql = f'COPY "{table}" TO STDOUT WITH CSV HEADER;'
        path = self.ensure_path(file)
        logger.info(f"Writing table {table} as csv to: {path}")
        with open(path, "w") as f:
            self.db_cur.copy_expert(sql, f)
        return path

    def ensure_container(self, container):
        self.objectstore_conn.put_container(container)

    def upload_file(self, container, file, output_file):
        logger.info(f"Uploading {file} to container {container} as {output_file}")
        with open(file, "rb") as f:
            self.objectstore_conn.put_object(container, output_file, f)

    def ensure_path(self, path):
        full_path = os.path.abspath(os.path.join(self.config.output_dir, path))
        dirname = os.path.dirname(full_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return full_path


if __name__ == "__main__":

    input_container = "parkeerrechten_pgdumps"
    output_container = "parkeerrechten_csvdumps"
    table_name = "BACKUP_VW_0363"

    exporter = Exporter()
    exporter.ensure_container(output_container)

    input_files = exporter.list_files(input_container, r"\d+_NPR_BACKUP.dump")
    output_files = exporter.list_files(output_container, r"\d+_NPR_BACKUP.csv")

    for file in input_files:
        base, ext = os.path.splitext(file["name"])
        output_file = f"{base}.csv"

        if any([x for x in output_files if x["name"] == output_file]):
            logger.info(
                f"Skipping {file['name']} since the output file {output_file} exists"
            )
            continue

        # Download the pg dump
        dump_path = exporter.download_file(input_container, file, overwrite=True)

        # Import the dump
        exporter.restore_table(dump_path)

        # Write the CSV
        csv_path = exporter.dump_table_csv(table_name, output_file)

        # Upload the CSV file
        exporter.upload_file(output_container, csv_path, os.path.basename(output_file))

        os.remove(csv_path)
        os.remove(dump_path)
