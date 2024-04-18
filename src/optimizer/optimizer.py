from typing import List, Tuple, Optional, Dict

from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.expressions import LogicalSqlOperators
from postbound.qal.predicates import as_predicate, CompoundPredicate
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection, GroupBy, CrossProduct, \
    Relation, Rename, SemiJoin, AntiJoin

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
        all_dependent_columns = self._find_all_dependent_columns(t1, t2)

        # 3. in die Form Dependent-join konvertieren
        dependent_join = self._convert_to_dependent_join(t1, t2)

        # 4. D berechnen
        d = self._derive_domain_node(dependent_join, all_dependent_columns)

        # 5. Push-Down

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
        """
        Derives outer query (T1) and subquery (T2) from the given subquery node.
        """
        t2 = subquery.input_node.mutate(as_root=True)

        # CrossProduct
        parent_node = subquery.parent_node
        t1 = None

        if parent_node:
            for child in parent_node.children():
                if child != t2:
                    t1 = child.mutate(as_root=True)
                    break

        return t1, t2

    def _update_relalg_structure(self, node: RelNode, **kwargs) -> RelNode:
        """
        Recursively updates the entire relational algebra structure beginning from the specified node.

        it proceeds to update the parent node, thereby ensuring that modifications are propagated throughout the entire tree structure.
        The process is repeated until the root of the tree is reached and updated, effectively updating the whole relational algebra structure.

        Parameters:
        - node: The starting node from which updates are to be propagated.
        - **kwargs: Additional arguments that may be required for updating nodes, such as modifying specific attributes of the nodes.
        """

        updated_node = node.mutate(**kwargs)

        if updated_node.parent_node is None:
            return updated_node

        parent_node = updated_node.parent_node
        if parent_node is None:
            return updated_node
        elif isinstance(parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
            if parent_node.left_input == node:
                updated_parent_node = self._update_relalg_structure(parent_node, left_child=updated_node)
            else:
                updated_parent_node = self._update_relalg_structure(parent_node, right_child=updated_node)
        elif isinstance(parent_node, (AntiJoin, SemiJoin)):
            if parent_node.input_node == node:
                updated_parent_node = self._update_relalg_structure(parent_node, input_node=updated_node)
            else:
                updated_parent_node = self._update_relalg_structure(parent_node, subquery_node=updated_node)
        else:
            updated_parent_node = self._update_relalg_structure(parent_node, input_node=updated_node)

        return next((child for child in updated_parent_node.children() if child == updated_node), updated_node)

    def _convert_to_dependent_join(self, t1: RelNode, t2: RelNode) -> RelNode:

        updated_t2 = None
        tab = TableReference("DummyTable", "DummyTable")
        col = ColumnReference("dummy", tab)
        dummy_rel = Relation(tab, [col])

        def find_and_remove_t1_in_t2(node: RelNode):
            nonlocal updated_t2
            if isinstance(node, Relation):
                return False
            children = node.children()
            if len(node.children()) == 0:
                return False

            if t1 in children and isinstance(node, CrossProduct):
                tail_node = node.right_input if node.left_input == t1 else node.left_input
                if isinstance(node.parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
                    if node.parent_node.left_input == node:
                        updated_t2 = self._update_relalg_structure(
                            node.parent_node, left_child=tail_node)
                    else:
                        updated_t2 = self._update_relalg_structure(
                            node.parent_node, right_child=tail_node)
                elif isinstance(node.parent_node, (SemiJoin, AntiJoin)):
                    if node.parent_node.input_node == node:
                        updated_t2 = self._update_relalg_structure(
                            node.parent_node, input_node=tail_node)
                    else:
                        updated_t2 = self._update_relalg_structure(
                            node.parent_node, subquery_node=tail_node)
                else:
                    updated_t2 = self._update_relalg_structure(
                        node.parent_node, input_node=tail_node)
                return True
            elif t1 in children and isinstance(node, ThetaJoin):
                if node.left_input == t1:
                    updated_t2 = self._update_relalg_structure(node, left_child=dummy_rel)
                else:
                    updated_t2 = self._update_relalg_structure(node, right_child=dummy_rel)
                return True

            for child in children:
                if find_and_remove_t1_in_t2(child):
                    return True

            return False

        find_and_remove_t1_in_t2(t2)

        dependent_join = DependentJoin(t1, updated_t2.root())
        updated_dependent_join = self._update_relalg_structure(dependent_join.mutate())
        return updated_dependent_join

    def _derive_domain_node(self, dependent_join: DependentJoin, all_dependent_columns: List[ColumnReference]) -> \
            Optional[RelNode]:
        t1 = dependent_join.left_input
        t2 = dependent_join.right_input

        join_predicates = []
        predicates_dict = {}

        tab_d = TableReference("domain", "d")

        for column in all_dependent_columns:
            column_d = ColumnReference(column.name, tab_d)
            join_predicate = as_predicate(column_d, LogicalSqlOperators.Equal, column)

            predicates_dict[column] = column_d
            join_predicates.append(join_predicate)

        # Domain-node
        rename = Rename(t1, {}, parent_node=None)
        domain = Projection(rename, predicates_dict.values())
        updated_domain = self._update_relalg_structure(domain)

        # free variables of t2 (subquery) update to match the domain node
        updated_t2 = self._update_column_name(t2, predicates_dict)

        updated_dependent_join = self._update_relalg_structure(dependent_join, left_child=updated_domain,
                                                               right_child=updated_t2)
        compound_join_predicates = CompoundPredicate.create_and(join_predicates)
        return ThetaJoin(t1, updated_dependent_join, compound_join_predicates)

    def _update_column_name(self, node: RelNode, column_mapping: Dict[ColumnReference, ColumnReference]) -> RelNode:
        if isinstance(node, Relation):
            return node
        updated_node = node
        new_columns = []

        if isinstance(node, (GroupBy, Projection)):
            node_columns = node.group_columns if isinstance(node, GroupBy) else node.columns

            for sql_expr in node_columns:
                for column in sql_expr.itercolumns():
                    if column in column_mapping.keys():
                        new_columns.append(column_mapping[column])
                    else:
                        new_columns.append(column)

            updated_node = updated_node.mutate(targets=new_columns) if isinstance(node,
                                                                                  Projection) else updated_node.mutate(
                group_columns=new_columns)
        """
        elif isinstance(node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            for column in node.predicate.itercolumns():
                if column in column_mapping.keys():
                    new_columns.append(column_mapping[column])
                else:
                    new_columns.append(column)
            updated_node = node.mutate(predicate=CompoundPredicate.create_and(new_columns))
        """

        if isinstance(node, (ThetaJoin, CrossProduct, DependentJoin)):
            updated_link_child = self._update_column_name(updated_node.left_input, column_mapping)
            updated_right_child = self._update_column_name(updated_node.right_input, column_mapping)
            return updated_node.mutate(left_child=updated_link_child, right_child=updated_right_child)
        elif isinstance(node, (SemiJoin, AntiJoin)):
            updated_input_node = self._update_column_name(updated_node.input_node, column_mapping)
            updated_subquery_node = self._update_column_name(updated_node.subquery_node, column_mapping)
            return updated_node.mutate(input_node=updated_input_node, subquery_node=updated_subquery_node)
        else:
            updated_child = self._update_column_name(updated_node.input_node, column_mapping)
            return updated_node.mutate(input_node=updated_child)

    def _find_all_dependent_columns(self, base_node: RelNode, dependent_node: RelNode) -> List[ColumnReference]:
        """
        Finds all columns that are dependent between the given base node and dependent node.
        """

        if dependent_node == base_node:
            return []

        tables = base_node.tables()
        dependent_columns = []

        if isinstance(dependent_node, (Projection, GroupBy)):
            dependent_columns += self._extract_columns_from_simple_conditions(dependent_node, tables)
        elif isinstance(dependent_node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            dependent_columns += self._extract_columns_from_composite_conditions(dependent_node, tables)

        for child_node in dependent_node.children():
            dependent_columns += self._find_all_dependent_columns(child_node, base_node)

        dependent_columns = list(set(dependent_columns))
        return dependent_columns

    @staticmethod
    def _extract_columns_from_composite_conditions(node: Selection | ThetaJoin | DependentJoin | SemiJoin | AntiJoin,
                                                   tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        for column in node.predicate.itercolumns():
            if column.table.identifier() in tables_identifier:
                columns.append(column)
        return columns

    @staticmethod
    def _extract_columns_from_simple_conditions(node: GroupBy | Projection,
                                                tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        node_columns = node.group_columns if isinstance(node, GroupBy) else node.columns

        for sql_expr in node_columns:
            for column in sql_expr.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)
        return columns
