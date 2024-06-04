from collections import deque
from typing import List, Tuple, Optional, Dict

from postbound.qal import transform
from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.expressions import LogicalSqlOperators, ColumnExpression
from postbound.qal.predicates import as_predicate, CompoundPredicate
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection, GroupBy, CrossProduct, \
    Relation, Rename, SemiJoin, AntiJoin, Map
from postbound.util.dicts import frozendict

from src.optimizer.dependent_join import DependentJoin
from src.utils.utils import Utils


class Optimizer:
    def __init__(self, utils: Utils):
        self.utils = utils

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
        all_dependent_columns = self.utils.find_all_dependent_columns(t1, t2)

        # 3. in die Form Dependent-join konvertieren
        dependent_join = self._convert_to_dependent_join(t1, t2)

        # 4. D berechnen
        d = self._derive_domain_node(dependent_join, all_dependent_columns)
        updated_d = self._update_root_node(d, relalg.root())

        return t1, t2, dependent_join, updated_d.root()

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

    def _convert_to_dependent_join(self, left_node: RelNode, right_node: RelNode) -> Optional[DependentJoin]:

        updated_t2 = None
        tab = TableReference("DummyTable", "DummyTable")
        col = ColumnReference("dummy", tab)
        dummy_rel = Relation(tab, [col])

        def find_and_remove_t1_in_t2(t1: RelNode, t2: RelNode):
            nonlocal updated_t2
            if isinstance(t2, Relation):
                return False
            children = t2.children()
            if len(t2.children()) == 0:
                return False

            if t1 in children and isinstance(t2, CrossProduct):
                tail_node = t2.right_input if t2.left_input == t1 else t2.left_input
                if isinstance(t2.parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
                    if t2.parent_node.left_input == t2:
                        updated_t2 = t2.parent_node.mutate(left_input=tail_node)
                    else:
                        updated_t2 = t2.parent_node.mutate(right_input=tail_node)
                elif isinstance(t2.parent_node, (SemiJoin, AntiJoin)):
                    if t2.parent_node.input_node == t2:
                        updated_t2 = t2.parent_node.mutate(input_node=tail_node)
                    else:
                        updated_t2 = t2.parent_node.mutate(subquery_node=tail_node)
                else:
                    updated_t2 = t2.parent_node.mutate(input_node=tail_node)
                return True

            elif t1 in children and isinstance(t2, ThetaJoin):
                if t2.left_input == t1:
                    updated_t2 = t2.mutate(left_input=dummy_rel)
                else:
                    new_right_input = t2.left_input.mutate(as_root=True)
                    updated_t2 = t2.mutate(left_input=dummy_rel, right_input=new_right_input)
                return True

            for child in children:
                if find_and_remove_t1_in_t2(t1, child):
                    return True

            return False

        def recursive_find_and_remove(t1: RelNode, t2: RelNode) -> bool:
            if find_and_remove_t1_in_t2(t1, t2):
                return True
            if updated_t2 is None:
                for child in t1.children():
                    if recursive_find_and_remove(child, t2):
                        return True
            return False

        recursive_find_and_remove(left_node, right_node)

        if updated_t2 is None:
            return None

        dependent_join = DependentJoin(left_node, updated_t2.root())
        return dependent_join

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
        t1_without_parent = t1.mutate(as_root=True)
        rename = Rename(t1_without_parent, predicates_dict, parent_node=None)
        transformed_values = list(map(lambda x: ColumnExpression(x), predicates_dict.values()))
        domain = Projection(rename, transformed_values)

        # free variables of t2 (subquery) update to match the domain node
        updated_t2 = self._update_column_name(t2, predicates_dict)

        updated_dependent_join = dependent_join.mutate(left_input=domain, right_input=updated_t2)
        compound_join_predicates = CompoundPredicate.create_and(join_predicates)
        return ThetaJoin(updated_dependent_join.left_input.input_node.input_node, updated_dependent_join,
                         compound_join_predicates).mutate()

    def _update_column_name(self, node: RelNode, column_mapping: Dict[ColumnReference, ColumnReference]) -> RelNode:
        if isinstance(node, Relation):
            return node
        updated_node = node

        if isinstance(node, Projection):
            new_columns = []
            for sql_expr in node.columns:
                new_columns.append(transform.rename_columns_in_expression(sql_expr, column_mapping))

            updated_node = node.mutate(targets=tuple(new_columns))
        elif isinstance(node, GroupBy):
            new_aggregates = {}
            new_group_columns = []
            for key_set, value_set in node.aggregates.items():
                new_key_set = frozenset(
                    transform.rename_columns_in_expression(expr, column_mapping) for expr in key_set)

                new_value_set = frozenset(
                    transform.rename_columns_in_expression(expr, column_mapping) for expr in value_set)
                new_aggregates[new_key_set] = new_value_set

            for column in node.group_columns:
                new_group_columns.append(transform.rename_columns_in_expression(column, column_mapping))

            updated_node = node.mutate(group_columns=tuple(new_group_columns),
                                       aggregates=frozendict(new_aggregates))
        elif isinstance(node, Map):
            new_mappings = {}
            for key_set, value_set in node.mapping.items():
                new_key_set = frozenset(
                    transform.rename_columns_in_expression(expr, column_mapping) for expr in key_set)

                new_value_set = frozenset(
                    transform.rename_columns_in_expression(expr, column_mapping) for expr in value_set)
                new_mappings[new_key_set] = new_value_set

            updated_node = node.mutate(mapping=frozendict(new_mappings))

        elif isinstance(node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            new_predicate = transform.rename_columns_in_predicate(node.predicate, column_mapping)
            updated_node = node.mutate(predicate=new_predicate)

        if isinstance(node, (ThetaJoin, CrossProduct, DependentJoin)):
            updated_link_child = self._update_column_name(updated_node.left_input, column_mapping)
            updated_right_child = self._update_column_name(updated_node.right_input, column_mapping)
            return updated_node.mutate(left_input=updated_link_child, right_input=updated_right_child)
        elif isinstance(node, (SemiJoin, AntiJoin)):
            updated_input_node = self._update_column_name(updated_node.input_node, column_mapping)
            updated_subquery_node = self._update_column_name(updated_node.subquery_node, column_mapping)
            return updated_node.mutate(input_node=updated_input_node, subquery_node=updated_subquery_node)
        else:
            updated_child = self._update_column_name(updated_node.input_node, column_mapping)
            return updated_node.mutate(input_node=updated_child)

    @staticmethod
    def _update_root_node(node: RelNode, root_node: RelNode) -> RelNode:
        queue = deque(root_node.children())

        while queue:
            current = queue.popleft()
            if isinstance(current, SubqueryScan):
                return current.parent_node.parent_node.mutate(input_node=node)

            queue.extend(current.children())

        return node
