import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import re
from collections import deque

from postbound.qal import transform, clauses, predicates, qal
from postbound.qal.base import TableReference, ColumnReference
from postbound.qal.expressions import SubqueryExpression, MathematicalExpression, FunctionExpression, SqlExpression
from postbound.qal.predicates import AbstractPredicate
from postbound.qal.relalg import RelNode, ThetaJoin, Relation, Projection, GroupBy, Selection
from src.utils.utils import Utils


class QueryGenerator:
    def __init__(self, utils: Utils):
        self.utils = utils

    def generate_sql_from_relalg(self, node: RelNode, subquery_root_node: RelNode):
        subquery_root = self._find_subquery_root_node(node.root(), subquery_root_node)
        t1 = subquery_root.left_input

        # 1) "with outerquery AS (),": In diese Klammern kommt die Abfrage für t1.
        sql_outerquery, outerquery_relations = self._generate_outer_query(t1)

        # sidepass von t1 lesen - Hier die Spaltennamen aus dem domain extrahieren.
        domain_columns_name = [ColumnReference(col.name) for col in next(iter(t1.sideways_pass)).mapping.keys()]

        # 2) dup_elim_outerquery AS (SELECT DISTINCT Spaltennamen from outerquery)
        sql_dup_elim = self._generate_dup_elim_outer_query(domain_columns_name)

        cte_clause = clauses.CommonTableExpression([sql_outerquery, sql_dup_elim])

        # 3) SELECT oberste Projektion FROM outerquery oq JOIN( hier leer lassen ) AS subquery ON ( leer lassen )
        #    WHERE wenn selection unter der Projektion vorhanden ist, dann die Bedingung dort einfügen
        sql_main_query, _ = self._generate_simple_select_query(node, stop_node=subquery_root,
                                                               additional_relations=[
                                                                   TableReference("outerquery", "oq")])

        sql_main_query_renamed = self._rename_columns_in_main_query(sql_main_query, outerquery_relations)
        # 3-1) den leeren Join in 3) mit dem rechten Kindknoten des ersten Joins füllen
        sql_sub_query, agg_mapping = self._generate_sub_query(subquery_root.right_input,
                                                              stop_node=next(iter(
                                                                  subquery_root.left_input.sideways_pass)).parent_node)

        # 3-2) Die ON-Bedingung nach dem Join in 3) sollte das Prädikat des ersten Joins sein.
        #      Im Prädikat 'd' mit 'oq' ersetzen und den Rest mit 'subquery'.
        sql_join_predicate = self._extract_join_predicate(subquery_root)
        sql_main_query_with_join = self._add_join_to_query(sql_main_query_renamed, sql_sub_query, sql_join_predicate)
        sql_main_query_renamed_subquery = self._rename_subquery(sql_main_query_with_join, agg_mapping)

        # 1, 2, 3 in einer Zeichenkette zusammenführen und zurückgeben
        return qal.SqlQuery(cte_clause=cte_clause, select_clause=sql_main_query_renamed_subquery.select_clause,
                            from_clause=sql_main_query_renamed_subquery.from_clause,
                            where_clause=sql_main_query_renamed_subquery.where_clause,
                            groupby_clause=sql_main_query_renamed_subquery.groupby_clause)

    @staticmethod
    def _find_subquery_root_node(node: RelNode, subquery_root_node: RelNode):
        queue = deque([node])
        while queue:
            current = queue.popleft()
            if str(current) == str(subquery_root_node):
                return current
            queue.extend(current.children())

    def _generate_outer_query(self, node: RelNode) -> (clauses.WithQuery, list[clauses.DirectTableSource]):
        inner_query, relations = self._generate_simple_select_query(node,
                                                                    column_generator=self.utils.find_all_dependent_columns)
        outer_query = clauses.WithQuery(inner_query, "outerquery")
        return outer_query, relations

    @staticmethod
    def _generate_dup_elim_outer_query(distinct_columns: [ColumnReference]) -> clauses.WithQuery:
        outerquery = TableReference("outerquery")
        select_clause = clauses.Select([clauses.BaseProjection(col) for col in distinct_columns],
                                       clauses.SelectType.SelectDistinct)
        from_clause = clauses.From([clauses.DirectTableSource(outerquery)])

        sql_dup_elim = qal.SqlQuery(select_clause=select_clause, from_clause=from_clause)

        outer_query = clauses.WithQuery(sql_dup_elim, "dup_elim_outerquery")
        return outer_query

    def _extract_join_predicate(self, join_node: ThetaJoin):
        tab_d = TableReference("domain", "d")
        tab_oq = TableReference("outerquery", "oq")
        tab_subquery = TableReference("subquery", "subquery")

        column_mapping = {}

        for column in join_node.predicate.itercolumns():
            if column.table.identifier() == tab_d.identifier():
                column_oq = ColumnReference(column.name, tab_oq)
                column_mapping[column] = column_oq
            else:
                column_sub = ColumnReference(column.name, tab_subquery)
                column_mapping[column] = column_sub

        return transform.rename_columns_in_predicate(join_node.predicate, column_mapping)

    def _generate_sub_query(self, node: RelNode, *, stop_node: RelNode = None) -> (
            qal.SqlQuery, list):
        select_projections = []
        where_conditions = []
        groupby_columns = []
        queue = deque([node])

        relations = []
        agg_mapping = {}
        agg_count = 1

        dup_elim_outerquery = TableReference("dup_elim_outerquery", "d")
        relations.append(clauses.DirectTableSource(dup_elim_outerquery))

        while queue:
            current = queue.popleft()

            if isinstance(current, Relation):
                relations.append(clauses.DirectTableSource(current.table))
            elif isinstance(current, Projection):
                columns = []
                for col in current.columns:
                    if re.search(r'\b(AVG|SUM|COUNT|MIN|MAX)\b', str(col), re.IGNORECASE):
                        alias = f"m{agg_count}"
                        columns.append(clauses.BaseProjection(col, alias))
                        agg_mapping[col] = alias
                        agg_count += 1
                    else:
                        columns.append(clauses.BaseProjection(col))

                select_projections += columns
            elif isinstance(current, GroupBy):
                groupby_columns.append(current.group_columns)

            elif isinstance(current, (ThetaJoin, Selection)):
                where_conditions.append(current.predicate)

            if not stop_node or stop_node not in current.children():
                queue.extend(current.children())
            else:
                children_without_stop_node = [child for child in current.children() if child != stop_node]
                queue.extend(children_without_stop_node)

        select_clause = clauses.Select(select_projections)
        from_clause = clauses.From(relations)
        where_clause = clauses.Where(
            predicates.CompoundPredicate.create_and(where_conditions)) if where_conditions else None

        flat_groupby_columns = [col for sublist in groupby_columns for col in sublist] if groupby_columns else []
        groupby_clause = clauses.GroupBy(flat_groupby_columns) if flat_groupby_columns else None

        sql_query = qal.SqlQuery(select_clause=select_clause, from_clause=from_clause, where_clause=where_clause,
                                 groupby_clause=groupby_clause)

        return sql_query, agg_mapping

    @staticmethod
    def _generate_simple_select_query(node: RelNode, *, column_generator=None, stop_node=None,
                                      additional_relations: [TableReference] = None) -> (
            qal.SqlQuery, list[clauses.DirectTableSource]):
        select_projections = []
        where_conditions = []
        groupby_columns = []
        queue = deque([node])

        relations = []

        if additional_relations:
            relations += [clauses.DirectTableSource(table) for table in additional_relations]

        while queue:
            current = queue.popleft()

            if current == stop_node:
                continue

            if isinstance(current, Relation):
                relations.append(clauses.DirectTableSource(current.table))
            elif isinstance(current, Projection):
                columns = [clauses.BaseProjection(col) for col in current.columns]
                select_projections += columns
            elif isinstance(current, GroupBy):
                groupby_columns.append(current.group_columns)

            elif isinstance(current, (ThetaJoin, Selection)):
                where_conditions.append(current.predicate)
            queue.extend(current.children())

        if column_generator:
            additional_columns = column_generator(node, node.root())
            select_projections += [clauses.BaseProjection(col) for col in additional_columns]

        select_clause = clauses.Select(select_projections)
        from_clause = clauses.From(relations)
        where_clause = clauses.Where(
            predicates.CompoundPredicate.create_and(where_conditions)) if where_conditions else None

        flat_groupby_columns = [col for sublist in groupby_columns for col in sublist] if groupby_columns else []
        groupby_clause = clauses.GroupBy(flat_groupby_columns) if flat_groupby_columns else None

        sql_query = qal.ImplicitSqlQuery(select_clause=select_clause, from_clause=from_clause,
                                         where_clause=where_clause,
                                         groupby_clause=groupby_clause)

        return sql_query, relations

    @staticmethod
    def _add_join_to_query(base_query: qal.SqlQuery, sub_query: qal.SqlQuery,
                           on_condition: AbstractPredicate) -> qal.SqlQuery:
        subquery_source = clauses.SubqueryTableSource(sub_query, "subquery")

        if base_query.from_clause:
            new_from_items = list(base_query.from_clause.items) + [subquery_source]
        else:
            new_from_items = [subquery_source]
        from_clause = clauses.From(new_from_items)

        if base_query.where_clause:
            new_where_condition = predicates.CompoundPredicate.create_and(
                [base_query.where_clause.predicate, on_condition])
        else:
            new_where_condition = on_condition
        where_clause = clauses.Where(new_where_condition)

        new_query = qal.SqlQuery(select_clause=base_query.select_clause, from_clause=from_clause,
                                 where_clause=where_clause, groupby_clause=base_query.groupby_clause)
        return new_query

    @staticmethod
    def _rename_subquery(main_query: qal.SqlQuery, agg_mapping: dict) -> qal.SqlQuery:

        where_clause = main_query.where_clause
        select_clause = main_query.select_clause
        rename_mapping = {}
        subquery_table = TableReference("subquery")

        if where_clause:
            for predicate in where_clause.predicate.iterexpressions():
                if isinstance(predicate, SubqueryExpression):
                    select_column = predicate.query.select_clause.targets[0].expression
                    if isinstance(select_column, (MathematicalExpression, FunctionExpression)):
                        new_column = ColumnReference(agg_mapping[select_column], subquery_table)
                    else:
                        new_column = ColumnReference(select_column.column.name, subquery_table)

                    rename_mapping[predicate] = new_column

        if select_clause:
            for target in select_clause.targets:
                expr = target.expression
                if isinstance(expr, SubqueryExpression):
                    select_column = expr.query.select_clause.targets[0].expression
                    if isinstance(select_column, (MathematicalExpression, FunctionExpression)):
                        new_column = ColumnReference(agg_mapping[select_column], subquery_table)
                    else:
                        new_column = ColumnReference(select_column.column.name, subquery_table)

                    rename_mapping[expr] = new_column

        def rename_expression(expression: SqlExpression) -> SqlExpression:
            if isinstance(expression, SubqueryExpression) and expression in rename_mapping.keys():
                return rename_mapping[expression]
            return expression

        if rename_mapping:
            main_query = transform.replace_expressions(main_query, rename_expression)

        return main_query

    @staticmethod
    def _rename_columns_in_main_query(sql_query: qal.SqlQuery,
                                      relations: list[clauses.DirectTableSource]) -> qal.SqlQuery:
        tab_oq = TableReference("outerquery", "oq")

        for relation in relations:
            sql_query = transform.rename_table(sql_query, relation.table, tab_oq)

        return sql_query
