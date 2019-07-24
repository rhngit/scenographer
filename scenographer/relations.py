"""
RelationDAG
"""

from functools import lru_cache
from typing import Iterable, List, Mapping, NamedTuple, Tuple, Type

import matplotlib.pyplot as plot
import networkx
from loguru import logger
from networkx import DiGraph
from sqlalchemy import ForeignKey, MetaData
from sqlalchemy.schema import Column, Table
from sqlalchemy_utils import get_referencing_foreign_keys

from scenographer.database import Database, UUIDField


class Relation(NamedTuple):
    "Container for the associated pair of primary key and foreign key"
    pk: Column
    fk: Column

    @property
    def edge(self) -> Tuple[Table, Table, Mapping[str, Column]]:
        "Returns a tuple that is accepted by networkx lib as a graph edge"
        return (self.pk.table, self.fk.table, {"relation": self})

    def __repr__(self) -> str:
        "REPL representation of a relation"
        relation_kwargs = [
            f"pk=database_tables.{self.pk.table.name}.c.{self.pk.name}",
            f"fk=database_tables.{self.fk.table.name}.c.{self.fk.name}",
        ]

        return f"Relation({','.join(relation_kwargs)})"

    @classmethod
    def from_foreign_key(cls: Type["Relation"], foreign_key: ForeignKey) -> "Relation":
        "Constructor for a relation given a ForeignKey object"
        return cls(
            pk=foreign_key.column,  # referenced column
            fk=foreign_key.parent,  # referer column
        )

    @classmethod
    def from_tables(cls: Type["Relation"], tables: List[Table]) -> List["Relation"]:
        """
        Constructor for a list of relations given a table
        Creates one relation for each foreign_key constraint associated with the table
        """
        return [
            cls.from_foreign_key(fk)
            for table in tables
            for fk in get_referencing_foreign_keys(table)
        ]


class RelationDAG(NamedTuple):
    "Wrapper for operations around the graph of relations"
    graph: DiGraph

    @property
    @lru_cache()
    def tables(self) -> List[Table]:
        "Lists all tables taken into consideration for sampling"
        return self.graph.nodes

    @property
    @lru_cache()
    def entrypoints(self) -> List[Table]:
        "Lists tables which have no foreign keys"
        return [n for (n, d) in self.graph.in_degree(self.graph.nodes) if d == 0]

    @property
    @lru_cache()
    def topologically_sorted(self) -> Iterable[Table]:
        """
        Returns all tables ordered in a way that
        if table X has a foreign key to Y, Y will always come first
        """
        return networkx.topological_sort(self.graph)

    def write_plot(self, filepath: str = "graph.png") -> None:
        "Meh graph image representation"
        logger.debug("Writing image file with graph")
        plot.title("RelationDAG")
        pos = networkx.drawing.nx_agraph.graphviz_layout(self.graph, prog="dot")
        plot.figure(figsize=(40, 40))

        networkx.draw_networkx_nodes(self.graph, pos, node_size=5000, alpha=0.9)
        networkx.draw_networkx_edges(
            self.graph, pos, arrows=True, node_size=5000,
        )
        networkx.draw_networkx_labels(self.graph, pos)

        plot.axis("off")
        plot.savefig(filepath)

    def write_dot(self, filepath: str = "graph.dot") -> None:
        "Meh graph image representation. File must be rendered with `dot`"
        logger.debug("Writing dot file with graph")
        networkx.drawing.nx_agraph.write_dot(self.graph, filepath)

    @property
    @lru_cache()
    def key_schema(self) -> MetaData:
        """
        Create schema with only the primary keys and foreign keys of each table.
        Ensures that the resulting schema is compatible with sqlite3
        """
        metadata = MetaData()

        for table in self.graph.nodes:

            # Here we use edge data for the first (only ?) time.
            # Perhaps we can shape the data better to avoid doing work here.

            # Select the relations whose foreign keys are present in this table
            relations = [
                edge[-1]["relation"]
                for edge in self.graph.reverse(copy=False).edges(
                    nbunch=table, data=True
                )
            ]

            # We get PK from table data instead of relation data,
            # because a primary key doesn't necessarily form a relation.
            # Assumes PK either is a single column or it doesn't exist.
            primary_key = [
                Column(c.name, c.type, primary_key=True)
                for c in table.primary_key.columns
            ][:1]

            columns = [
                *primary_key,
                *[
                    Column(r.fk.name, r.fk.type, ForeignKey(str(r.pk)))
                    for r in relations
                ],
            ]

            # Map postgres UUIDs into sqlite compatible UUIDs
            for column in columns:
                if str(column.type) not in ["UUID", "BIGINT", "INTEGER"]:
                    logger.warning(
                        "Key column {} has weird type {}",
                        f"{table.name}.{column.name}",
                        str(column.type),
                    )
                if str(column.type) == "UUID":
                    column.type = UUIDField()

            Table(table.name, metadata, *columns)

        return metadata

    @classmethod
    def from_graph(cls: Type["RelationDAG"], graph: DiGraph) -> "RelationDAG":
        """
        Instanciates a RelationDAG from a networkx DiGraph.
        It makes the graph is immutable and raises if the graph is not a DAG
        """
        new = cls(networkx.freeze(graph))
        if networkx.is_directed_acyclic_graph(new.graph):
            logger.debug(
                "DAG contains {} nodes and {} edges",
                len(graph.nodes),
                len(graph.edges),
            )
        else:
            logger.error("Generated graph is not a DAG.")
            raise ValueError

        return new

    @classmethod
    def load(
        cls: Type["RelationDAG"],
        database: Database,
        extend_relations: List[Relation],
        ignore_relations: List[Relation],
        ignore_tables: List[Table],
    ) -> "RelationDAG":
        """
        Create a RelationDAG
        The data loaded from this method is sourced
        from the database and from the user config
        """
        graph = DiGraph(name="RelationDAG")

        # Get actual table instances
        tables = database.tables.__dict__.values()

        # Create relations from table data and add the ones specified in settings
        relations = Relation.from_tables(tables) + extend_relations

        # Create graph
        graph.add_nodes_from(tables)
        graph.add_edges_from([r.edge for r in relations])

        # Remove excluded entities (tables and relations) from the created graph
        graph.remove_edges_from([r.edge for r in ignore_relations])
        graph.remove_nodes_from(ignore_tables)

        # Create RelationDAG instance
        return cls.from_graph(graph)

    def __str__(self) -> str:
        "Return some useful information about the graph"
        return networkx.info(self.graph)

    def __repr__(self) -> str:
        "For REPL use. Should work with only RelationDAG in scope."
        return "RelationDAG"
