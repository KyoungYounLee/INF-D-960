from typing import List, Tuple, Optional

from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection, GroupBy, CrossProduct

from src.optimizer.dependent_join import DependentJoin


class Optimizer:
    def __init__(self):
        pass

    def optimize_unnesting(self, relalg: RelNode):
        """
        1. Prüfen, ob die Abfrage einen abhängigen Join hat
        2. Subquery (T2), Outerquery (T1) berechnen
        3. in die Form Dependent-join konvertieren
        4. D berechnen
        5. Push-dpwn
        """

        # 1. Prüfen, ob die Abfrage einen abhängigen Join hat
        local_rel_nodes = self._find_dependent_subquery_node(relalg)
        if len(local_rel_nodes) == 0:
            return relalg

        # 2. Subquery (T2), Outerquery (T1) berechnen
        t1, t2, root = self._derive_outer_and_sub_with_root(local_rel_nodes[0])

        # 3. in die Form Dependent-join konvertieren
        dependent_join = self._convert_to_dependent_join(t1, t2)

        # 4. D berechnen

        all_dependent_columns = None
        if t1:
            all_dependent_columns = self._find_all_dependent_columns(t2, t1)

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
    def _derive_outer_and_sub_with_root(subquery: SubqueryScan) -> Tuple[Optional[RelNode], RelNode]:
        t2 = subquery.input_node.mutate(as_root=True)

        # CrossProduct
        parent_node = subquery.parent_node
        t1 = None
        root = None

        if parent_node:
            for child in parent_node.children():
                if child != t2:
                    t1 = child.mutate(as_root=True)
                    break

        root = parent_node.mutate(parent_node.left_child, None)

        return t1, t2, root

    # TODO: Das sollte den ganzen Baum aktualisieren.
    def _update_parent_nodes_upward(self, child: RelNode, new_child: RelNode) -> RelNode:
        parent_node = child.parent_node
        # Type 정리해서 다 넣기
        if isinstance(parent_node, ThetaJoin or CrossProduct or DependentJoin):
            if parent_node.left_input == child:
                parent_node = parent_node.mutate(left_child=new_child)
            elif parent_node.right_input == child:
                parent_node = parent_node.mutate(right_child=new_child)
        elif isinstance(parent_node, Projection or Selection or GroupBy):
            if parent_node.input_node == child:
                parent_node = parent_node.mutate(input_node=new_child)

        return parent_node if parent_node.root() is True else self._update_parent_nodes_upward(child.parent_node,
                                                                                               parent_node)

    @staticmethod
    def _convert_to_dependent_join(t1: RelNode, t2: RelNode) -> RelNode:
        dependent_join = DependentJoin(t1, t2)
        updated_dependent_join = dependent_join.mutate()

        dependent_join_parent_node = t2.parent_node.parent_node.parent_node.mutate(input_node=updated_dependent_join)

        new_root = dependent_join_parent_node.parent_node.mutate(input_node=dependent_join_parent_node, as_root=True)
        return new_root

    def _insert_domain_D(self, dependent_join: DependentJoin):
        all_dependent_columns = self._find_all_dependent_columns(dependent_join.base_node,
                                                                 dependent_join.dependent_node)

        def find_t2_node_in_t1(t1: RelNode, t2: RelNode):
            for child in t1.children():
                pass

        pass

    def _find_all_dependent_columns(self, base_node: RelNode, dependent_node: RelNode) -> List[ColumnReference]:
        if dependent_node == base_node:
            return []

        tables = base_node.tables()
        dependent_columns = []

        if isinstance(dependent_node, Projection):
            dependent_columns += self._find_all_dependent_projection_columns(dependent_node, tables)
        elif isinstance(dependent_node, ThetaJoin):
            dependent_columns += self._find_all_dependent_join_columns(dependent_node, tables)
        elif isinstance(dependent_node, GroupBy):
            dependent_columns += self._find_all_dependent_groupby_columns(dependent_node, tables)
        elif isinstance(dependent_node, Selection):
            dependent_columns += self._find_all_dependent_selection_columns(dependent_node, tables)

        for child_node in dependent_node.children():
            dependent_columns += self._find_all_dependent_columns(child_node, base_node)

        dependent_columns = list(set(dependent_columns))
        return dependent_columns

    @staticmethod
    def _find_all_dependent_projection_columns(project: Projection, tables: frozenset[TableReference]) \
            -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for sql_expr in project.columns:
            for column in sql_expr.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_join_columns(join: ThetaJoin, tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for column in join.predicate.itercolumns():
            if column.table.identifier() in tables_identifier:
                columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_groupby_columns(groupby: GroupBy, tables: frozenset[TableReference]) -> List[
        ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for sql_expr in groupby.group_columns:
            for column in sql_expr.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)

        return columns

    @staticmethod
    def _find_all_dependent_selection_columns(selection: Selection, tables: frozenset[TableReference]) -> List[
        ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for column in selection.predicate.itercolumns():
            if column.table.identifier() in tables_identifier:
                columns.append(column)
        return columns
