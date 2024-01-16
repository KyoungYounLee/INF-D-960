from typing import Optional, List

from postbound.db.db import DatabaseSchema
from postbound.qal.base import TableReference
from postbound.qal.clauses import Where
from postbound.qal.parser import parse_query
from postbound.qal.qal import SqlQuery

from src.data_model.relational_algebra.groupby import GroupBy
from src.data_model.relational_algebra.join import Join
from src.data_model.relational_algebra.node import Node
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

    def _convert_implicit_sql_to_relational_algebra(self, sql_query: SqlQuery) -> RelationalAlgebraQuery:
        select_clause = sql_query.select_clause
        from_clause = sql_query.from_clause
        where_clause = sql_query.where_clause
        groupby_clause = sql_query.groupby_clause

        if where_clause and any(condition.is_join() for condition in where_clause.predicate.base_predicates()):
            child_node = self._convert_implicit_join_to_relational_algebra(where_clause)
        elif where_clause:
            table_references = from_clause.tables()
            table_node = Table(table_references.pop())
            child_node = Select(table_node, where_clause.predicate)
        else:
            table_references = from_clause.tables()
            table_node = Table(table_references.pop())
            child_node = table_node

        if groupby_clause:
            aggregate_function = ("field_name", "function_name")  # 적절한 집계 함수로 수정 필요
            child_node = GroupBy(child_node, groupby_clause.group_columns, aggregate_function)

        project_node = Project(child_node, select_clause.columns())

        return RelationalAlgebraQuery(project_node)

        # join_node = Join(base table, join table, condition, join-type)
        # 1) join base table, join table
        # 2) subquery - z.B) join base table in der Form Subquery
        # 3) rekursive Funktion

    def _convert_implicit_join_to_relational_algebra(self, where_clause: Where) -> Node:
        abstract_predicate = where_clause.predicate
        join_nodes = []
        filter_conditions = []

        for predicate in abstract_predicate.base_predicates():
            if predicate.is_join():
                tables = []
                for table in predicate.tables():
                    tables.append(Table(table))
                join_nodes.append(Join(tables[0], tables[1], predicate))
            elif predicate.is_filter():
                filter_conditions.append(predicate)

        for join_node in join_nodes:
            new_children = []
            for child in join_node.children:
                filter_condition = next((cond for cond in filter_conditions if child.table in cond.tables()), None)
                if filter_condition:
                    new_children.append(Select(child, filter_condition))
                else:
                    new_children.append(child)
            join_node.children = new_children
        return self._join_nodes_to_one_relational_algebra(join_nodes)

    @staticmethod
    def _get_all_tables(root_node: Node) -> List[TableReference]:
        tables = []

        def traverse(node):
            if isinstance(node, Table):
                tables.append(node)
                return

            for child in node.children:
                traverse(child)

        traverse(root_node)
        return tables

    def _join_nodes_to_one_relational_algebra(self, join_nodes: List[Join]) -> Node:
        def process_node(node, remaining_joins):
            if not remaining_joins:
                return node

            new_children = []
            for child in node.children:
                for join in remaining_joins[:]:
                    if child in self._get_all_tables(join):
                        new_children.append(join)
                        remaining_joins.remove(join)
                        break
                else:
                    new_children.append(child)

                new_children = [process_node(c, remaining_joins) for c in new_children]

            node.children = new_children
            return node

        root_node = join_nodes.pop(0)
        return process_node(root_node, join_nodes)
