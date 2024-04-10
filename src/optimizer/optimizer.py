from typing import List, Tuple, Optional

from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection, GroupBy, CrossProduct, \
    Relation

from src.optimizer.dependent_join import DependentJoin


class Optimizer:
    def __init__(self):
        pass

    def optimize_unnesting(self, relalg: RelNode):
        """
        1. Prüfen, ob die Abfrage einen abhängigen Join hat
        2. Outerquery (T1), Subquery (T2) berechnen
        3. in die Form Dependent-join konvertieren
        4. D berechnen
        5. Push-dpwn
        """

        # 1. Prüfen, ob die Abfrage einen abhängigen Join hat
        local_rel_nodes = self._find_dependent_subquery_node(relalg)
        if len(local_rel_nodes) == 0:
            return relalg

        # 2. Outerquery (T1), Subquery (T2) berechnen
        t1, t2 = self._derive_outer_and_sub_query(local_rel_nodes[0])

        # 3. in die Form Dependent-join konvertieren
        dependent_join = self._convert_to_dependent_join(t1, t2)

        # 4. D berechnen
        d = self._derive_domain_node(t1, t2)

        return t1, t2, dependent_join, d

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

    def _derive_outer_and_sub_query(self, subquery: SubqueryScan) -> Tuple[Optional[RelNode], RelNode]:
        t2 = self._update_relalg_structure(subquery.input_node.mutate(as_root=True))

        # CrossProduct
        parent_node = subquery.parent_node
        t1 = None

        if parent_node:
            for child in parent_node.children():
                if child != t2:
                    t1 = self._update_relalg_structure(child.mutate(as_root=True))
                    break

        return t1, t2

    def _update_node_and_all_children_nodes(self, node: RelNode, updated_nodes_set=None) -> Optional[RelNode]:
        if updated_nodes_set is None:
            updated_nodes_set = set()

        if node in updated_nodes_set or isinstance(node, Relation):
            return node

        if isinstance(node, (ThetaJoin, CrossProduct, DependentJoin)):
            updated_link_child = self._update_node_and_all_children_nodes(node.left_input)
            updated_right_child = self._update_node_and_all_children_nodes(node.right_input)
            return node.mutate(left_child=updated_link_child, right_child=updated_right_child)
        else:
            updated_child = self._update_node_and_all_children_nodes(node.input_node)
            return node.mutate(input_node=updated_child)

    def _update_relalg_structure(self, node: RelNode, updated_nodes_set=None, **kwargs) -> RelNode:
        if updated_nodes_set is None:
            updated_nodes_set = set()

        updated_node = self._update_node_and_all_children_nodes(node.mutate(**kwargs), updated_nodes_set)
        updated_nodes_set.add(updated_node)

        if updated_node.parent_node is None:
            return updated_node

        parent_node = updated_node.parent_node
        if isinstance(parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
            if parent_node.left_input == node:
                return self._update_relalg_structure(parent_node.mutate(left_child=updated_node), updated_nodes_set)
            else:
                return self._update_relalg_structure(parent_node.mutate(right_child=updated_node), updated_nodes_set)
        else:
            return self._update_relalg_structure(parent_node.mutate(input_node=updated_node), updated_nodes_set)

    def _convert_to_dependent_join(self, t1: RelNode, t2: RelNode) -> RelNode:
        dependent_join = DependentJoin(t1, t2)
        updated_dependent_join = self._update_relalg_structure(dependent_join.mutate())
        return updated_dependent_join

    def _derive_domain_node(self, t1: RelNode, t2: RelNode) -> Optional[RelNode]:
        domain = None
        all_dependent_columns = self._find_all_dependent_columns(t2, t1)

        domain = Projection(t1, all_dependent_columns)
        return self._update_relalg_structure(domain.mutate())

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
