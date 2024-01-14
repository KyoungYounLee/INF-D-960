from typing import Optional

from postbound.db.db import DatabaseSchema
from postbound.qal.parser import parse_query
from postbound.qal.qal import SqlQuery

from src.data_model.relational_algebra.join import Join
from src.data_model.relational_algebra.project import Project
from src.data_model.relational_algebra.select import Select
from src.data_model.relational_algebra.table import Table
from src.data_model.relational_algebra_query import RelationalAlgebraQuery


class Parser:
    def __init__(self):
        pass

    def parse(self, sql_query: str, bind_columns: Optional[bool] = None,
              db_schema: Optional[DatabaseSchema] = None) -> SqlQuery:
        return parse_query(sql_query, bind_columns=bind_columns, db_schema=db_schema)

    def convert_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        root_node = self._convert_implicit_sql_to_relational_algebra(sql_query)
        return root_node

    @staticmethod
    def _convert_implicit_sql_to_relational_algebra(sql_query: SqlQuery) -> RelationalAlgebraQuery:
        select_clause = sql_query.select_clause
        from_clause = sql_query.from_clause
        where_cluase = sql_query.where_clause

        table_references = from_clause.tables()
        table_node = Table(table_references.pop())
        select_node = Select(table_node, where_cluase.predicate)
        project_node = Project(select_node, select_clause.columns())

        return RelationalAlgebraQuery(project_node)

        #join_node = Join(base table, join table, condition, join-type)
        # 1) join base table, join table
        # 2) subquery - z.B) join base table in der Form Subquery
        # 3) rekursive Funktion

        # When die Tables mehr als eins sind
        # table_references = from_clause.tables()
        # table_nodes = [Table(table_ref.full_name) for table_ref in table_references]

        # JOIN -> Join-Node of Relational Algebra
        # SELECT -> Project, WHERE -> Select


    @staticmethod
    def _convert_select_to_project(sql_query: SqlQuery, child) -> Project:
        return Project(child, sql_query.select_clause.columns())

