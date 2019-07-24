"""
This module takes care of loading the settings, transforming them
into an appropriate format if necessary and providing the
user-provided settings to the rest of the application.
"""

import json
import operator
import os
import sys
from functools import lru_cache
from pathlib import Path
from tempfile import mkdtemp
from typing import Any, Iterable, List, Mapping, NamedTuple, Optional

import sqlalchemy
from dict_digger import dig
from loguru import logger
from pyrsistent import freeze, thaw
from sqlalchemy.schema import Table

from scenographer.database import Database
from scenographer.relations import Relation, RelationDAG
from scenographer.sample import TableSample


class Settings(NamedTuple):
    SOURCE_DATABASE_URL: str
    TARGET_DATABASE_URL: str
    QUERY_MODIFIERS: dict = freeze({})
    IGNORE_TABLES: List[str] = freeze([])
    EXTEND_RELATIONS: List[Mapping[str, str]] = freeze([])
    IGNORE_RELATIONS: List[Mapping[str, str]] = freeze([])
    OUTPUT_DIRECTORY: Optional[str] = None

    @classmethod
    def load(cls, path):
        import commentjson

        with open(path) as f:
            data = commentjson.load(f)

        return cls(**freeze(data))

    @classmethod
    @lru_cache()
    def empty(cls):
        return cls(
            SOURCE_DATABASE_URL="",
            TARGET_DATABASE_URL="",
            IGNORE_TABLES=freeze(["example1", "migrations"]),
            EXTEND_RELATIONS=freeze(
                [{"pk": "product.id", "fk": "product_ownership.product_id"}]
            ),
            IGNORE_RELATIONS=freeze(
                [{"pk": "product.id", "fk": "client.favorite_product_id"}]
            ),
            QUERY_MODIFIERS=freeze(
                {
                    "_default": {"conditions": [], "limit": 300},
                    "users": {"conditions": ["email ilike '%@example.com'"]},
                }
            ),
        )

    @property
    @lru_cache()
    def json(self):
        return json.dumps(
            {k: thaw(v) for k, v in self._asdict().items()}, indent=4, sort_keys=True,
        )


class Scenographer(NamedTuple):
    options: Settings

    @property
    @lru_cache()
    def source_database(self) -> Database:
        """
        SOURCE_DATABASE_URL key.
        Abort if any any issue arises!
        """
        return self._instanciate_database("SOURCE_DATABASE_URL")

    @property
    def target_database(self) -> Database:
        """
        TARGET_DATABASE_URL key.
        Abort if any any issue arises!
        """
        return self._instanciate_database("TARGET_DATABASE_URL")

    @property
    @lru_cache()
    def output_directory(self) -> Path:
        """
        SOURCE_DATABASE_URL key.
        Abort if any any issue arises!
        """
        if not self.options.OUTPUT_DIRECTORY:
            directory = mkdtemp(prefix="sample-")
            logger.info(
                "Output directory unspecified. "
                "Saving samples in temporary directory {}",
                directory,
            )
        else:
            directory = self.options.OUTPUT_DIRECTORY
            try:
                os.mkdir(directory)
            except FileExistsError:
                logger.error(
                    "Error creating output directory {}. "
                    "Make sure there's nothing there. "
                    "Aborting.",
                    directory,
                )
                sys.exit(1)

        return Path(directory)

    @property
    @lru_cache()
    def extend_relations(self) -> Iterable[Relation]:
        """
        EXTEND_RELATIONS key.
        Scenographer will recognize the relations specified here,
        in addition to those specified by database constraints.
        This method will return the equivalent Relation instances
        """
        relations = []
        for relation in self.options.EXTEND_RELATIONS:
            try:
                relations.append(
                    Relation(
                        pk=self.column_by_name[relation["pk"]],
                        fk=self.column_by_name[relation["fk"]],
                    )
                )
            except KeyError:
                logger.warning("Can't match relation {}. Skipping.", dict(relation))
                continue

        return relations

    @property
    @lru_cache()
    def ignore_relations(self) -> Iterable[Relation]:
        """
        IGNORE_RELATIONS key.
        Scenographer will not recognize the relations specified here.
        This method will return the equivalent Relation instances
        """
        relations = []
        for relation in self.options.IGNORE_RELATIONS:
            try:
                relations.append(
                    Relation(
                        pk=self.column_by_name[relation["pk"]],
                        fk=self.column_by_name[relation["fk"]],
                    )
                )
            except KeyError:
                logger.warning("Can't match relation {}. Skipping.", relation)
                continue

        return relations

    @property
    @lru_cache()
    def ignore_tables(self) -> Iterable[Table]:
        """
        IGNORE_TABLES key.
        Scenographer will not sample any data coming from these tables.
        This method will return the equivalent Table instances
        (binded to source_database)
        """
        ignored_tables = []
        for table_name in self.options.IGNORE_TABLES:
            if table_name in self.source_database.tables.__dict__:
                ignored_tables.append(getattr(self.source_database.tables, table_name))
            else:
                logger.warning("Can't find table {}. Skipping.", table_name)
        return ignored_tables

    @property
    @lru_cache()
    def query_modifiers(self) -> Mapping[Table, Mapping[str, Any]]:
        """
        QUERY_MODIFIERS key.
        Scenographer will take this key in account while sampling.
        This method will return the equivalent Table instances
        (binded to source_database)
        """
        mods = self.options.QUERY_MODIFIERS
        default_limit = dig(thaw(mods), "_default", "limit") or 30
        default_conditions = dig(thaw(mods), "_default", "conditions") or list()

        non_specified_entrypoints = operator.sub(
            set([t.name for t in self.relation_dag.entrypoints]), set(mods.keys()),
        )
        if non_specified_entrypoints:
            logger.warning(
                "Entrypoints are advised to be added as query modifiers. "
                "They define what the final sample will look like"
            )
            logger.warning(
                "These entrypoints are not specified: {}", non_specified_entrypoints,
            )

        modifiers = {}
        for table in self.relation_dag.tables:
            if table.name not in mods:
                limit = default_limit
                conditions = default_conditions

            else:
                limit, conditions = (
                    mods[table.name].get("limit"),
                    mods[table.name].get("conditions"),
                )
                if not limit and not conditions:
                    logger.warning("QUERY_MODIFIER for {} malformed.", table.name)
                    continue

                limit = limit or default_limit
                conditions = conditions or default_conditions

            modifiers[table] = {"limit": limit, "conditions": conditions}

        return modifiers

    @property
    @lru_cache()
    def relation_dag(self) -> RelationDAG:
        logger.info("Building the database graph")
        return RelationDAG.load(
            self.source_database,
            extend_relations=self.extend_relations,
            ignore_relations=self.ignore_relations,
            ignore_tables=self.ignore_tables,
        )

    @property
    def samples(self) -> Iterable[Table]:
        logger.info("Starting sampling")
        return TableSample.sample_dag(
            self.source_database,
            self.relation_dag,
            self.query_modifiers,
            self.output_directory,
        )

    @property
    @lru_cache()
    def column_by_name(self):
        return {
            f"{column.table.name}.{column.name}": column
            for columns in [
                table.columns for table in self.source_database.tables.__dict__.values()
            ]
            for column in columns
        }

    def copy_schema(self):
        logger.info("Copying schema")
        self.target_database.load_schema(self.source_database)
        return self

    def copy_sample(self):
        logger.info("Loading sample into target")
        self.target_database.load_samples(self.output_directory, self.samples)
        return self

    def _instanciate_database(self, var):
        """
        Helper method to instanciate a database method
        Scenographer will read a postgres url from the config
        and try to connect with that database.
        Abort if any any issue arises!
        """
        try:
            database_url = self.options._asdict()[var]
            return Database(database_url).test_conn()
        except sqlalchemy.exc.ArgumentError:
            logger.error("Error connecting to {}", var)
            sys.exit(1)
