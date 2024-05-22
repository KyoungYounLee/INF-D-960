import re
from collections import deque

from postbound.qal import transform
from postbound.qal.base import TableReference, ColumnReference
from postbound.qal.relalg import RelNode, ThetaJoin, Relation, Projection, GroupBy, Selection

from src.utils.utils import Utils


class QueryGenerator:
    def __init__(self, utils: Utils):
        self.utils = utils

    def generate_sql_from_relalg(self, node: RelNode):
        # annehmen, dass der linke Kindknoten im ersten Join t1 ist.
        first_join = self._find_first_join_node(node.root())
        t1 = first_join.left_input

        # 1) "with outerquery AS (),": In diese Klammern kommt die Abfrage für t1.
        sql_outerquery, outerquery_relations = self._generate_outer_query(t1)

        # sidepass von t1 lesen - Hier die Spaltennamen aus dem domain extrahieren.
        domain_columns_name = [col.name for col in next(iter(t1.sideways_pass)).mapping.keys()]
        distinct_columns = ', '.join(domain_columns_name)

        # 2) dup_elim_outerquery AS (SELECT DISTINCT Spaltennamen from outerquery)
        sql_dup_elim = f"dup_elim_outerquery AS (SELECT DISTINCT {distinct_columns} FROM outerquery)"

        # 3) SELECT oberste Projektion FROM outerquery oq JOIN( hier leer lassen ) AS subquery ON ( leer lassen )
        #    WHERE wenn selection unter der Projektion vorhanden ist, dann die Bedingung dort einfügen
        sql_main_query, _ = self._generate_simple_select_query(node, stop_node=first_join,
                                                               additional_relations=[
                                                                   TableReference("outerquery", "oq")])
        # 3-1) den leeren Join in 3) mit dem rechten Kindknoten des ersten Joins füllen
        sql_sub_query, agg_mapping = self._generate_sub_query(first_join.right_input,
                                                              stop_node=next(iter(
                                                                  first_join.left_input.sideways_pass)).parent_node,
                                                              domain_columns=domain_columns_name)

        # 3-2) Die ON-Bedingung nach dem Join in 3) sollte das Prädikat des ersten Joins sein.
        #      Im Prädikat 'd' mit 'oq' ersetzen und den Rest mit 'subquery'.
        sql_join_predicate = str(self._extract_join_predicate(first_join))
        sql_main_query_with_join = self._add_join_to_query(sql_main_query, "() as subquery", sql_join_predicate)
        sql_main_query_renamed_subquery = self._rename_subquery_columns(sql_main_query_with_join, agg_mapping)
        sql_main_query_without_subquery = self._rename_columns_in_main_query(sql_main_query_renamed_subquery,
                                                                             outerquery_relations)

        sql_main_and_sub_query = re.sub(r'JOIN \(\)', f'JOIN (\n\t{sql_sub_query})', sql_main_query_without_subquery)

        # 1, 2, 3 in einer Zeichenkette zusammenführen und zurückgeben
        sql_final = f"{sql_outerquery},\n{sql_dup_elim}\n{sql_main_and_sub_query}"
        return sql_final

    @staticmethod
    def _find_first_join_node(node: RelNode):
        queue = deque([node])
        while queue:
            current = queue.popleft()
            if isinstance(current, ThetaJoin):
                return current
            queue.extend(current.children())

    def _generate_outer_query(self, node: RelNode) -> (str, list):
        inner_query, relations = self._generate_simple_select_query(node,
                                                                    column_generator=self.utils.find_all_dependent_columns,
                                                                    tab_line=True)
        outer_query = f"WITH outerquery AS (\n\t{inner_query})"

        return outer_query, relations

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

    def _generate_sub_query(self, node: RelNode, *, stop_node: RelNode = None, domain_columns=None) -> (str, list):
        select_part = "SELECT "
        from_part = "FROM "
        where_part = ""
        groupby_part = ""
        queue = deque([node])

        relations = []
        where_conditions = []
        agg_mapping = {}
        agg_count = 1
        has_group_by = False

        dup_elim_outerquery = TableReference("dup_elim_outerquery", "d")
        relations.append(dup_elim_outerquery)

        while queue:
            current = queue.popleft()

            if isinstance(current, Relation):
                relations.append(current.table)
            elif isinstance(current, Projection):
                columns = []
                for col in current.columns:
                    col_str = str(col)
                    if re.search(r'\b(AVG|SUM|COUNT|MIN|MAX)\b', col_str, re.IGNORECASE):
                        alias = f"m{agg_count}"
                        columns.append(f"{col_str} AS {alias}")
                        agg_mapping[col_str] = alias
                        agg_count += 1
                    else:
                        columns.append(col_str)

                select_part += ', '.join(columns)
            elif isinstance(current, GroupBy):
                has_group_by = True
                grouping_columns = ', '.join([str(col) for col in current.group_columns])
                if grouping_columns:
                    groupby_part = f"GROUP BY {grouping_columns}"

            elif isinstance(current, (ThetaJoin, Selection)):
                where_conditions.append(str(current.predicate))

            if not stop_node or stop_node not in current.children():
                queue.extend(current.children())
            else:
                children_without_stop_node = [child for child in current.children() if child != stop_node]
                queue.extend(children_without_stop_node)

        where_part = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        from_part += ", ".join(
            [f"{relation.full_name} {relation.alias}" for relation in relations if relation.full_name != "DummyTable"])

        # Todo: Muss geändert werden, sobald die Pushdown-Regel abgeschlossen ist
        if domain_columns:
            domain_columns_with_prefix = [f"d.{col}" for col in domain_columns]
            if select_part.endswith("SELECT "):
                select_part += ', '.join(domain_columns_with_prefix)
            else:
                select_part += ', ' + ', '.join(domain_columns_with_prefix)

            if has_group_by:
                if groupby_part.startswith("GROUP BY "):
                    groupby_part += ', ' + ', '.join(domain_columns_with_prefix)
                else:
                    groupby_part += 'GROUP BY ' + ', '.join(domain_columns_with_prefix)

        sql_query = f"{select_part}\n\t{from_part}\n\t{where_part}\n\t{groupby_part}"

        return sql_query.strip(), agg_mapping

    def _generate_simple_select_query(self, node: RelNode, *, column_generator=None, stop_node=None,
                                      additional_relations: [TableReference] = None, tab_line=False) -> (str, list):
        select_part = "SELECT "
        from_part = "FROM "
        where_part = ""
        groupby_part = ""
        queue = deque([node])

        relations = []
        aggregation_functions = []
        where_conditions = []

        if additional_relations:
            relations += additional_relations

        while queue:
            current = queue.popleft()

            if current == stop_node:
                continue

            if isinstance(current, Relation):
                relations.append(current.table)
            elif isinstance(current, Projection):
                columns = ', '.join([str(col) for col in current.columns])
                select_part += columns
            elif isinstance(current, GroupBy):
                grouping_columns = ', '.join([str(col) for col in current.group_columns])
                if grouping_columns:
                    groupby_part = f"GROUP BY {grouping_columns}"

                for expr_set, function_set in current.aggregates.items():
                    for expr in expr_set:
                        for function in function_set:
                            aggregation_functions.append(f"{function}({expr})")

            elif isinstance(current, (ThetaJoin, Selection)):
                where_conditions.append(str(current.predicate))

            queue.extend(current.children())

        if aggregation_functions:
            if select_part.endswith("SELECT "):
                select_part += ', '.join(aggregation_functions)
            else:
                select_part += ', ' + ', '.join(aggregation_functions)

        where_part = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        from_part += ", ".join([f"{relation.full_name} {relation.alias}" for relation in relations])

        if column_generator:
            additional_columns = column_generator(node, node.root())
            select_part += ', '.join([str(col) for col in additional_columns])

        if tab_line:
            sql_query = f"{select_part}\n\t{from_part}\n\t{where_part}\n\t{groupby_part}"
        else:
            sql_query = f"{select_part}\n{from_part}\n{where_part}\n{groupby_part}"
        return sql_query.strip(), relations

    @staticmethod
    def _add_join_to_query(base_query: str, join_clause: str, on_condition: str) -> str:
        insert_position = base_query.upper().find("WHERE")
        if insert_position == -1:
            insert_position = len(base_query)

        new_query = base_query[:insert_position] + f"JOIN {join_clause} \nON {on_condition} \n" + base_query[
                                                                                                  insert_position:]
        return new_query.strip()

    @staticmethod
    def _rename_subquery_columns(subquery: str, agg_mapping: dict) -> str:
        subquery_pattern = re.compile(r"\(SELECT (.*?) FROM .*?\)", re.IGNORECASE | re.DOTALL)

        def rename_columns(match):
            columns_part = match.group(1)
            columns = [col.strip() for col in columns_part.split(',')]

            new_columns = []

            for col in columns:
                if col in agg_mapping.keys():
                    new_columns.append(f"subquery.{agg_mapping[col]}")
                else:
                    parts = col.split('.')
                    new_columns.append(f"subquery.{parts[1]}")

            new_columns_part = ', '.join(new_columns)
            return f"{new_columns_part}"

        match = subquery_pattern.search(subquery)
        if not match:
            return subquery

        renamed_subquery = subquery_pattern.sub(rename_columns, subquery)
        renamed_subquery = re.sub(r'\)+$', '', renamed_subquery)
        return renamed_subquery

    @staticmethod
    def _rename_columns_in_main_query(sql_query: str, relations: list) -> str:
        for relation in relations:
            pattern = re.compile(rf'\b{relation.alias}\b')
            sql_query = pattern.sub(f"oq", sql_query)
        return sql_query
