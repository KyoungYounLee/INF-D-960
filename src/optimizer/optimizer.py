from typing import List, Tuple, Optional, Dict

from postbound.qal import transform
from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.expressions import LogicalSqlOperators
from postbound.qal.predicates import as_predicate, CompoundPredicate
from postbound.qal.relalg import RelNode, SubqueryScan, Projection, ThetaJoin, Selection, GroupBy, CrossProduct, \
    Relation, Rename, SemiJoin, AntiJoin, Map

from src.optimizer.dependent_join import DependentJoin
from src.optimizer.push_down_manager import PushDownManager
from src.utils.utils import Utils


class Optimizer:
    def __init__(self, pushDownManager: PushDownManager, utils: Utils):
        self.pushDownManager = pushDownManager
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
        all_dependent_columns = self._find_all_dependent_columns(t1, t2)

        # 3. in die Form Dependent-join konvertieren
        dependent_join = self._convert_to_dependent_join(t1, t2)

        # 4. D berechnen
        d = self._derive_domain_node(dependent_join, all_dependent_columns)

        # 5. Push-Down
        # result = self.pushDownManager.push_down(d, all_dependent_columns)

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
                        updated_t2 = self.utils.update_relalg_structure_upward(
                            node.parent_node, left_child=tail_node)
                    else:
                        updated_t2 = self.utils.update_relalg_structure_upward(
                            node.parent_node, right_child=tail_node)
                elif isinstance(node.parent_node, (SemiJoin, AntiJoin)):
                    if node.parent_node.input_node == node:
                        updated_t2 = self.utils.update_relalg_structure_upward(
                            node.parent_node, input_node=tail_node)
                    else:
                        updated_t2 = self.utils.update_relalg_structure_upward(
                            node.parent_node, subquery_node=tail_node)
                else:
                    updated_t2 = self.utils.update_relalg_structure_upward(
                        node.parent_node, input_node=tail_node)
                return True
            elif t1 in children and isinstance(node, ThetaJoin):
                if node.left_input == t1:
                    updated_t2 = self.utils.update_relalg_structure_upward(node, left_child=dummy_rel)
                else:
                    updated_t2 = self.utils.update_relalg_structure_upward(node, right_child=dummy_rel)
                return True

            for child in children:
                if find_and_remove_t1_in_t2(child):
                    return True

            return False

        find_and_remove_t1_in_t2(t2)

        dependent_join = DependentJoin(t1, updated_t2.root())
        updated_dependent_join = self.utils.update_relalg_structure_upward(dependent_join.mutate())
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
        t1_without_parent = t1.mutate(as_root=True)
        rename = Rename(t1_without_parent, {}, parent_node=None)
        domain = Projection(rename, predicates_dict.values())

        # free variables of t2 (subquery) update to match the domain node
        updated_t2 = self._update_column_name(t2, predicates_dict)

        updated_dependent_join = dependent_join.mutate(left_child=domain, right_child=updated_t2)
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
                new_columns.append(transform._rename_columns_in_expression(sql_expr, column_mapping))

            updated_node = self.utils.update_relalg_structure_upward(node, targets=new_columns)
        elif isinstance(node, GroupBy):
            new_aggregates = {}
            new_group_columns = []
            for key_set, value_set in node.aggregates.items():
                new_key_set = frozenset(
                    transform._rename_columns_in_expression(expr, column_mapping) for expr in key_set)

                new_value_set = frozenset(
                    transform._rename_columns_in_expression(expr, column_mapping) for expr in value_set)
                new_aggregates[new_key_set] = new_value_set

            for column in node.group_columns:
                new_group_columns.append(transform._rename_columns_in_expression(column, column_mapping))

            updated_node = self.utils.update_relalg_structure_upward(node, group_columns=new_group_columns,
                                                                     aggregates=new_aggregates)
        elif isinstance(node, Map):
            new_mappings = {}
            for key_set, value_set in node.mapping.items():
                new_key_set = frozenset(
                    transform._rename_columns_in_expression(expr, column_mapping) for expr in key_set)

                new_value_set = frozenset(
                    transform._rename_columns_in_expression(expr, column_mapping) for expr in value_set)
                new_mappings[new_key_set] = new_value_set

            updated_node = self.utils.update_relalg_structure_upward(node, mapping=new_mappings)

        elif isinstance(node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            new_predicate = transform.rename_columns_in_predicate(node.predicate, column_mapping)
            updated_node = node.mutate(predicate=new_predicate)

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

        if isinstance(dependent_node, (Map, Projection, GroupBy)):
            dependent_columns += self._extract_columns_from_simple_conditions(dependent_node, tables)
        elif isinstance(dependent_node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            dependent_columns += self._extract_columns_from_composite_conditions(dependent_node, tables)

        for child_node in dependent_node.children():
            dependent_columns += self._find_all_dependent_columns(base_node, child_node)

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
    def _extract_columns_from_simple_conditions(node: Map | GroupBy | Projection,
                                                tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        if isinstance(node, (GroupBy, Projection)):
            node_columns = node.group_columns if isinstance(node, GroupBy) else node.columns

            for sql_expr in node_columns:
                for column in sql_expr.itercolumns():
                    if column.table.identifier() in tables_identifier:
                        columns.append(column)

        if isinstance(node, (GroupBy, Map)):
            disc = node.aggregates if isinstance(node, GroupBy) else node.mapping
            for key_set, value_set in disc.items():
                for key_expr in key_set:
                    for key_column in key_expr.itercolumns():
                        if key_column.table.identifier() in tables_identifier:
                            columns.append(key_column)

                for value_expr in value_set:
                    for value_column in value_expr.itercolumns():
                        if value_column.table.identifier() in tables_identifier:
                            columns.append(value_column)

        return columns
