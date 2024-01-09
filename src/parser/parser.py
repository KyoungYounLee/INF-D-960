from typing import Optional

from postbound.db.db import DatabaseSchema
from postbound.qal.parser import parse_query
from postbound.qal.qal import SqlQuery

from src.data_model.relational_algebra.join import Join
from src.data_model.relational_algebra.project import Project
from src.data_model.relational_algebra_query import RelationalAlgebraQuery


class Parser:
    def __init__(self):
        pass

    def parse(self, sql_query: str, bind_columns: Optional[bool] = None,
              db_schema: Optional[DatabaseSchema] = None) -> SqlQuery:
        return parse_query(sql_query, bind_columns=bind_columns, db_schema=db_schema)

    def convert_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        root_node = self._convert_query_based_on_type(sql_query)
        return root_node

    def _convert_query_based_on_type(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        is_explicit = sql_query.is_explicit()
        is_implicit = sql_query.is_implicit()

        if is_explicit and is_implicit:
            return self._convert_mixed_sql_to_relational_algebra(sql_query)
        elif is_explicit:
            return self._convert_explicit_sql_to_relational_algebra(sql_query)
        elif is_implicit:
            return self._convert_implicit_sql_to_relational_algebra(sql_query)
        else:
            raise ValueError("Unrecognized SQL query type")

    def _convert_explicit_sql_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        # join_node = Join(sql_query.from_clause base table, join table, condition, join-type)
        # 1) join base table, join table
        # 2) subquery - z.B) join base table in der Form Subquery
        # 3) rekursive Funktion

        # JOIN -> Join-Node of Relational Algebra
        # SELECT -> Project, WHERE -> Select
        pass

    def _convert_implicit_sql_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        # Wie kann ich unterscheiden, was Join oder einfach ein where condition ist?

        # FROM -> Table node
        # WHERE 절의 조인 조건은 Join 연산으로 재구성. 이는 두 테이블 간의 관계를 분석하여 적절한 Join 노드를 생성
        pass

    def _convert_mixed_sql_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        # was bedeutet hier ein mixed-type-sql?
        # FROM 절을 분석할 때, 각 테이블 참조가 명시적인지 암시적인지를 결정
        pass

    @staticmethod
    def _convert_select_to_project(sql_query: SqlQuery, child) -> Project:
        return Project(child, sql_query.select_clause.columns())

