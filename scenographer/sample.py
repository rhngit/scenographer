"TableSample"

import os
import sys
from csv import DictReader, field_size_limit
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, List, NamedTuple

from loguru import logger
from pyrsistent import freeze
from sqlalchemy import select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import Column, Table
from sqlalchemy.sql.expression import Select

from scenographer.database import Database
from scenographer.relations import RelationDAG


class TableSample(NamedTuple):
    """
    TableSample is a wrapper around the operations
    to sample a single table
    """

    key_database = Database("sqlite://")

    table: Table
    conditions: Iterable[str]
    limit: int

    @classmethod
    def sample_dag(
        cls,
        source_database: Database,
        relations: RelationDAG,
        query_modifiers: Any,
        directory: Path,
    ) -> List["TableSample"]:
        "Samples a database according to its relation graph"

        # Change current working dir to our output directory
        # This way we avoid TableSample instances containing that information
        os.chdir(directory)

        # Prepare the sqlite db
        relations.key_schema.drop_all(cls.key_database.engine)
        relations.key_schema.create_all(cls.key_database.engine)

        samples = []
        for table in relations.topologically_sorted:
            sample = cls(
                table,
                conditions=freeze(query_modifiers[table]["conditions"]),
                limit=query_modifiers[table]["limit"],
            ).sample(source_database)

            samples.append(sample.table)
        return samples

    # @classmethod
    # @lru_cache()
    # def key_database(cls, relations) -> Database:
    #     return Database("sqlite://")

    @property
    @lru_cache(None)
    def key_table(self) -> Table:
        """
        Returns the table with the associated metadata
        tied to the key database
        """
        return getattr(self.key_database.tables, self.table.name)

    @property
    @lru_cache(None)
    def source_table(self) -> Table:
        """
        Returns the table with the associated metadata
        tied to the source database
        """
        return self.table

    @property
    @lru_cache(None)
    def key_columns(self) -> Iterable[Column]:
        """
        Property defining the columns of the key database:
        the primary_keys and foreign_keys of each table
        """
        return self.key_table.columns

    @property
    @lru_cache(None)
    def foreign_keys(self) -> Iterable[Column]:
        """
        Returns the foreign keys of the table by listing
        the key columns and excluding any primary_key
        """
        return [c for c in self.key_columns if not c.primary_key]

    @property
    @lru_cache(None)
    def is_entrypoint(self) -> bool:
        """
        Property defining if the table has foreign_keys.
        If it is false it means it has no dependencies.
        """
        return not bool(list(self.foreign_keys))

    @property
    @lru_cache(None)
    def query(self) -> Select:
        "SQLAlchemy query object for the sampling query"
        query = self.table.select()
        for condition in self.conditions:
            query = query.where(text(condition))

        if not self.is_entrypoint:
            query = self.follow_conditions(query)

        query = query.limit(self.limit)

        return query

    def follow_conditions(self, query: Select) -> Select:
        "Append WHERE clauses to restrict the rows depending on the data already extracted"
        foreign_key: Column
        primary_key: Column

        # For each foreign_key, build a where clause in the form of:
        #   WHERE sometable_id in (<select id from sometable>)
        # The subquery is executed on the key database and its results are
        # parametrized into the final query.
        for foreign_key in self.foreign_keys:

            primary_key = iter(foreign_key.foreign_keys).__next__().column
            pk_data = self.key_database.execute_return_list(select([primary_key]))
            source_fk = getattr(self.source_table.c, foreign_key.name)

            if pk_data:
                query = query.where(
                    source_fk.in_(pk_data) | source_fk.is_(None)
                ).order_by(source_fk.nullslast())
            else:
                query = query.where(source_fk.is_(None))

        return query

    @property
    @lru_cache(None)
    def sql(self) -> str:
        "Raw SQL representation of the sampling query"
        complete_query = self.query.compile(
            dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}
        ).__str__()

        # Sampling requires we always extract all the columns, but
        # complete_query comes with the entire columns list.
        # So let's replace the column list with a wildcard:
        query_str = "SELECT * " + complete_query[complete_query.index("FROM") :]
        return query_str

    @property
    @lru_cache(None)
    def persisted_records_path(self) -> Path:
        """
        The path for the csv file for the data to be written in
        Relative to classmethod `sample_dag` directory argument.
        """
        return Path(self.table.name).with_suffix(".csv")

    def persist_keys(self) -> None:
        "Insert retrieved keys from the table csv file into the sqlite key database."
        with self.persisted_records_path.open(newline="") as csv_file:
            keys = [
                {key.name: persisted_record[key.name] for key in self.key_columns}
                for persisted_record in DictReader(csv_file)
            ]
            logger.debug("Got {} records for {}.", len(keys), self.table.name)

        if keys:
            self.key_database.execute(self.key_table.insert(), keys)

    def persist_records(self, source_database) -> None:
        "Insert retrieved rows into persisted_records_path"
        logger.trace("Querying {}\n{}", self.table.name, self.sql)

        with self.persisted_records_path.open("a+") as csv_file:
            source_database.copy_to_csv(csv_file, self.query)

    def sample(self, source_database):
        "Samples the table"
        self.persist_records(source_database)
        self.persist_keys()

        return self


def increase_csv_limit():
    """
    This is needed, due to default limits.
    Taken from https://stackoverflow.com/a/15063941
    """
    max_int = sys.maxsize

    while True:
        # decrease the max_int value by factor 10
        # as long as the OverflowError occurs.

        try:
            field_size_limit(max_int)
            break
        except OverflowError:
            max_int = int(max_int / 10)


increase_csv_limit()
