from typing import List, Tuple, Optional

from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection


class Optimizer:
    def __init__(self):
        pass

    def optimize_unnesting(self, relalg: RelNode):
        """
        Checken ob die Query einen abhängigen Join hat.
        Subquery (T2), Outerquery (T1) berechnen
        D berechnen
        Abhängigen Join in Unnesting-Form umwandeln
        """

        local_rel_nodes = self._find_dependent_subquery_node(relalg)
        if len(local_rel_nodes) == 0:
            return relalg

        t1, t2 = self._derive_sub_and_outer_queries(local_rel_nodes[0])

        return t1, t2

    @staticmethod
    def _find_dependent_subquery_node(relalg: RelNode) -> List[SubqueryScan]:
        """
        This function finds and returns dependent join and subjoin within the given relational algebra representation (relalg).

        :param relalg: The root node of the relational algebra tree to be inspected.
        :return: A list of SubqueryScan nodes that are dependent.
        """
        subqueries = []

        def find_dependent_subqueries(relalg: RelNode):
            for child in relalg.children() or []:
                if isinstance(child, SubqueryScan):
                    if child.subquery.is_dependent():
                        subqueries.append(child)
                find_dependent_subqueries(child)

        find_dependent_subqueries(relalg)
        return subqueries

    @staticmethod
    def _derive_sub_and_outer_queries(subquery: SubqueryScan) -> Tuple[Optional[RelNode], RelNode]:
        t2 = subquery.input_node

        parent_node = subquery.parent_node
        t1 = None

        if parent_node:
            for child in parent_node.children():
                if child != t2:
                    t1 = child
                    break
        return t1, t2

    def _derive_domain_D(self):
        pass

    def _find_all_dependent_columns(self, node: RelNode, tables: List[TableReference]) -> List[ColumnReference]:

        dependent_columns = []

        if isinstance(node, Projection):
            dependent_columns += self._find_all_dependent_projection_columns(node, tables)
        elif isinstance(node, ThetaJoin):
            dependent_columns += self._find_all_dependent_join_columns(node, tables)
        elif isinstance(node, GroupBy):
            dependent_columns += self._find_all_dependent_groupby_columns(node, tables)
        elif isinstance(node, Selection):
            dependent_columns += self._find_all_dependent_selection_columns(node, tables)

        for child_node in node.children():
            dependent_columns += self._find_all_dependent_columns(child_node, tables)

        return dependent_columns

    @staticmethod
    def _find_all_dependent_projection_columns(project: Projection, tables: List[TableReference]) \
            -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for sql_expr in project.columns:
            for column in sql_expr.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_join_columns(join: ThetaJoin, tables: List[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for column in join.predicate.itercolumns():
            if column.table.identifier() in tables_identifier:
                columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_groupby_columns(groupby: GroupBy, tables: List[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for sql_expr in groupby.group_columns:
            for column in sql_expr.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_selection_columns(selection: Selection, tables: List[TableReference]) -> List[
        ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for column in selection.predicate.itercolumns():
            if column.table.identifier() in tables_identifier:
                columns.append(column)
        return columns
