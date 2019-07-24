#!/usr/bin/env python3

"""
Module to connect with database
"""

import subprocess
import uuid
from functools import lru_cache
from pathlib import Path
from pipes import quote
from types import SimpleNamespace
from typing import Any, Iterable, List, Mapping, NamedTuple, TextIO

import postgres_copy
import sqlalchemy
from loguru import logger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine, ResultProxy
from sqlalchemy.schema import Table
from sqlalchemy.sql.expression import Select
from sqlalchemy.types import CHAR, TypeDecorator

from scenographer.utils import PrintAs


class Database(NamedTuple):
    "Wrapper around database operations"
    database_url: str

    def execute(self, *args, **kwargs) -> ResultProxy:
        """
        Executes query and returns the usual ResultProxy from SQLAlchemy.
        """
        with self.engine.begin() as connection:
            return connection.execute(*args, **kwargs)

    def execute_return_list(self, *args, **kwargs) -> List[Any]:
        """
        Executes query and returns a list of the resulting values
        It raises an AssertionError if more than one column is returned
        """
        resultproxy = list(self.execute(*args, **kwargs))

        if resultproxy:
            columns = [column_name for column_name, value in resultproxy[0].items()]
            if len(columns) != 1:
                print(columns)
            assert len(columns) == 1

        return [rowproxy.values()[0] for rowproxy in resultproxy]

    def execute_return_dict(self, *args, **kwargs) -> List[Mapping[str, Any]]:
        """
        Executes query and returns each row as a dictionary;
        It raises an AssertionError if any column name is repeated.
        """
        resultproxy = list(self.execute(*args, **kwargs))

        if resultproxy:
            columns = [column_name for column_name, value in resultproxy[0].items()]
            assert len(columns) == len(set(columns))

        return [
            {column: value for column, value in rowproxy.items()}
            for rowproxy in resultproxy
        ]

    def copy_to_csv(self, file: TextIO, select: Select) -> None:
        "Executes query and write the rows into a file object in CSV format."
        postgres_copy.copy_to(select, file, self.engine, format="csv", header=True)

    @property
    @lru_cache()
    def engine(self) -> Engine:
        "Create, return and cache the associated sqlalchemy engine"
        return sqlalchemy.create_engine(self.database_url)

    @property
    @lru_cache()
    def tables(self) -> SimpleNamespace:
        "Reflect the database to return and cache a namespace with all of its tables"

        logger.info("Reflecting source database")
        metadata = sqlalchemy.MetaData()
        with PrintAs(logger.warning):
            metadata.reflect(self.engine, views=False)

        return SimpleNamespace(**metadata.tables)

    def load_schema(self, source_database: "Database") -> None:
        """
        pg_dump \
            --format=custom --no-owner --schema-only \
            --verbose {source_database} \
        | pg_restore \
            --format=custom --no-owner --schema-only \
            --no-acl \
            --verbose -d {target_database}
        """
        pg_copy_schema = self.load_schema.__doc__.format(
            source_database=quote(source_database.database_url),
            target_database=quote(self.database_url),
        )
        process = subprocess.Popen(
            pg_copy_schema,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        with process.stdout:
            for line in iter(process.stdout.readline, b""):
                logger.trace("{}", line)

        exit_code = process.wait()
        logger.debug("Command pg_dump | pg_restore exited with code {}", exit_code)
        # assert exit_code == 0

    def load_samples(self, directory: Path, samples: Iterable[Table]) -> None:
        "Copy the generated sample CSVs into the database"
        for table in samples:
            with open(directory / Path(table.name).with_suffix(".csv")) as file:
                postgres_copy.copy_from(
                    file, table, self.engine, format="csv", header=True
                )

    def test_conn(self) -> "Database":
        "Copy the generated sample CSVs into the database"
        return self.engine and self  # Always returns self


# Retrieved from
# https://docs.sqlalchemy.org/en/13/core/custom_types.html#backend-agnostic-guid-type
class UUIDField(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.
    """

    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return "%.32x" % uuid.UUID(value).int
            else:
                # hexstring
                return "%.32x" % value.int

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value
