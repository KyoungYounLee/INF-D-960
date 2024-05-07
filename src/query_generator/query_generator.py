from collections import deque

from postbound.qal.relalg import RelNode, ThetaJoin, Relation, Projection, GroupBy, Selection


class QueryGenerator:
    def __init__(self):
        pass

    def generate_sql_from_relalg(self, node: RelNode) -> str:
        # annehmen, dass der linke Kindknoten im ersten Join t1 ist.
        first_join = self._find_first_join_node(node.root())
        t1 = first_join.left_input

        # 1) "with outerquery AS (),": In diese Klammern kommt die Abfrage für t1.
        sql_outerquery = self._generate_outer_query(t1)
        # sidepass von t1 lesen - Hier die Spaltennamen aus dem domain extrahieren.

        # 2) dup_elim_outerquery AS (SELECT DISTINCT Spaltennamen from outerquery)
        # 3) SELECT oberste Projektion FROM outerquery oq JOIN( hier leer lassen ) AS subquery ON ( leer lassen )
        #    WHERE wenn selection unter der Projektion vorhanden ist, dann die Bedingung dort einfügen
        # 3-1) den leeren Join in 3) mit dem rechten Kindknoten des ersten Joins füllen
        # 3-2) Die ON-Bedingung nach dem Join in 3) sollte das Prädikat des ersten Joins sein.
        #      Im Prädikat 'd' mit 'oq' ersetzen und den Rest mit 'subquery'.

        # 1, 2, 3 in einer Zeichenkette zusammenführen und zurückgeben
        return sql_outerquery

    @staticmethod
    def _find_first_join_node(node: RelNode):
        queue = deque([node])
        while queue:
            current = queue.popleft()
            if isinstance(current, ThetaJoin):
                return current
            queue.extend(current.children())

    def _generate_outer_query(self, node: RelNode) -> str:
        inner_query = self._generate_simple_select_query(node)
        outer_query = f"WITH outerquery AS ({inner_query})"

        return outer_query

    def _generate_distinct_outer_query(self, node: RelNode) -> str:
        return ""

    def _generate_main_query(self, node: RelNode) -> str:
        return ""

    def _generate_sub_query(self, node: RelNode, sql_query: str) -> str:
        return ""

    def _generate_simple_select_query(self, node: RelNode, column_generator=None) -> str:
        select_part = "SELECT "
        from_part = "FROM "
        where_part = ""
        groupby_part = ""
        queue = deque([node])

        relations = []
        aggregation_functions = []
        where_conditions = []

        if column_generator:
            additional_columns = column_generator(node)
            select_part += ', '.join([str(col) for col in additional_columns])

        while queue:
            current = queue.popleft()

            if isinstance(current, Relation):
                relations.append(current.table.full_name + " " + current.table.alias)
            elif isinstance(current, Projection):
                columns = ', '.join([str(col) for col in current.columns])
                select_part += columns
            elif isinstance(current, GroupBy):
                grouping_columns = ', '.join([str(col) for col in current.group_columns])
                if grouping_columns:
                    groupby_part = f" GROUP BY {grouping_columns}"

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

        where_part = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        from_part += ", ".join(relations)
        sql_query = f"{select_part} {from_part}{where_part}{groupby_part}"
        return sql_query.strip()
