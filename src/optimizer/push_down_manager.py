from typing import Optional, List

from postbound.qal import base
from postbound.qal.relalg import RelNode, Projection, GroupBy, Selection, ThetaJoin, SemiJoin, AntiJoin, Map, Relation, \
    CrossProduct

from src.optimizer.dependent_join import DependentJoin
from src.utils.utils import Utils


class PushDownManager:
    def __init__(self, utils: Utils):
        self.utils = utils

    def push_down(self, node: RelNode) -> RelNode:

        # Navigieren zur Position des Knotens für den dependent-Join node
        dependent_join = self._navigate_to_dependent_join(node)

        if dependent_join is None:
            return node.root()

        # Prüfen, ob das rechte Kind des dependent-join Knotens freie Variablen in der Baumstruktur hat
        dependent_node = dependent_join.right_input

        #  1) False
        #    - Die letzte Regel der Push-Down-Regel wird angewendet, der dependent-join-Knoten wird beseitigt und die Schleife wird beendet
        #  2) True
        #   - Anwenden eine Push-down-Regel für diesen Typ. An diesem Punkt wird die gesamte Baumstruktur aktualisiert
        #   - zurück zu Schritt 1 und wiederholen die Schleife
        if self._check_free_variables_in_node(dependent_node):
            updated_node = self._apply_push_down_rule(dependent_node)
        else:
            updated_node = self._apply_push_down_rule_final(dependent_node)
        return self.push_down(updated_node)

    def _check_free_variables_in_node(self, node: RelNode) -> bool:
        if isinstance(node, Relation):
            return False

        tab_d = base.TableReference("domain", "d")
        tab_d_identifier = tab_d.identifier()

        if isinstance(node, (Projection, GroupBy)):
            node_columns = node.group_columns if isinstance(node, GroupBy) else node.columns
            for sql_expr in node_columns:
                for column in sql_expr.itercolumns():
                    if column.table.identifier() is tab_d_identifier:
                        return True
        elif isinstance(node, (GroupBy, Map)):
            disc = node.aggregates if isinstance(node, GroupBy) else node.mapping
            for key_set, value_set in disc.items():
                for key_expr in key_set:
                    for key_column in key_expr.itercolumns():
                        if key_column.table.identifier() is tab_d_identifier:
                            return True

                for value_expr in value_set:
                    for value_column in value_expr.itercolumns():
                        if value_column.table.identifier() is tab_d_identifier:
                            return True

        elif isinstance(node, (Selection, ThetaJoin, SemiJoin, AntiJoin)):
            for column in node.predicate.itercolumns():
                if column.table.identifier() is tab_d_identifier:
                    return True

        for child in node.children():
            if self._check_free_variables_in_node(child):
                return True

        return False

    def _apply_push_down_rule(self, node: RelNode) -> RelNode:
        if isinstance(node, Selection):
            return self._push_down_rule_selection(node)
        elif isinstance(node, Projection):
            return self._push_down_rule_projection(node)
        elif isinstance(node, GroupBy):
            return self._push_down_rule_groupby(node)
        elif isinstance(node, Map):
            return self._push_down_rule_map(node)
        elif isinstance(node, ThetaJoin):
            return self._push_down_rule_join(node)

        return node

    def _push_down_rule_selection(self, node: Selection) -> RelNode:
        dependent_join = node.parent_node.mutate(as_root=True, right_child=node.input_node)
        updated_node = node.mutate(as_root=True, input_node=dependent_join)

        return self._push_down_dependent_join(node, updated_node)

    def _push_down_rule_projection(self, node: Projection) -> RelNode:
        additional_columns = node.parent_node.left_input.columns
        dependent_join = node.parent_node.mutate(as_root=True, right_child=node.input_node)
        updated_node = node.mutate(as_root=True, input_node=dependent_join, targets=node.columns + additional_columns)

        return self._push_down_dependent_join(node, updated_node)

    def _push_down_rule_map(self, node: GroupBy | Map) -> RelNode:
        dependent_join = node.parent_node.mutate(as_root=True, right_child=node.input_node)
        updated_node = node.mutate(as_root=True, input_node=dependent_join)

        return self._push_down_dependent_join(node, updated_node)

    def _push_down_rule_groupby(self, node: GroupBy | Map) -> RelNode:
        additional_columns = node.parent_node.left_input.columns
        dependent_join = node.parent_node.mutate(as_root=True, right_child=node.input_node)
        updated_node = node.mutate(as_root=True, input_node=dependent_join,
                                   group_columns=node.group_columns + additional_columns)

        return self._push_down_dependent_join(node, updated_node)

    def _push_down_rule_join(self, node: ThetaJoin) -> RelNode:
        if self._check_free_variables_in_node(node.right_input):
            dependent_join = node.parent_node.mutate(as_root=True, right_child=node.right_input)
            updated_node = node.mutate(as_root=True, right_child=dependent_join)
        else:
            dependent_join = node.parent_node.mutate(as_root=True, right_child=node.left_input)
            updated_node = node.mutate(as_root=True, left_child=dependent_join)

        return self._push_down_dependent_join(node, updated_node)

    def _apply_push_down_rule_final(self, node: RelNode):
        dependent_join = node.parent_node
        left_child = dependent_join.left_input.mutate(as_root=True)
        right_child = dependent_join.right_input.mutate(as_root=True)

        if isinstance(node, Relation) and node.table.full_name == "DummyTable":
            parent_node = dependent_join.parent_node
            updated_parent_node = parent_node.mutate(left_child=left_child)
            return self.utils.update_relalg_structure_upward(updated_parent_node)

        cross_product = CrossProduct(left_input=left_child, right_input=right_child,
                                     parent_node=dependent_join.parent_node)
        return self.utils.update_relalg_structure_upward(cross_product)

    def _push_down_dependent_join(self, node: RelNode, updated_node: RelNode) -> RelNode:
        dependent_join_parent_node = node.parent_node.parent_node

        if isinstance(dependent_join_parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
            if dependent_join_parent_node.left_input == node.parent_node:
                updated_parent = self.utils.update_relalg_structure_upward(
                    dependent_join_parent_node, left_child=updated_node)
            else:
                updated_parent = self.utils.update_relalg_structure_upward(
                    dependent_join_parent_node, right_child=updated_node)
        elif isinstance(dependent_join_parent_node, (SemiJoin, AntiJoin)):
            if dependent_join_parent_node.input_node == node:
                updated_parent = self.utils.update_relalg_structure_upward(
                    dependent_join_parent_node, input_node=updated_node)
            else:
                updated_parent = self.utils.update_relalg_structure_upward(
                    dependent_join_parent_node, subquery_node=updated_node)
        else:
            updated_parent = self.utils.update_relalg_structure_upward(
                dependent_join_parent_node, input_node=updated_node)

        return updated_parent

    def _navigate_to_dependent_join(self, node: RelNode) -> Optional[DependentJoin]:
        if isinstance(node, DependentJoin):
            return node

        for child in node.children():
            result = self._navigate_to_dependent_join(child)
            if result is not None:
                return result
        return None

    def _navigate_to_dependent_joins(self, node: RelNode) -> List[DependentJoin]:
        dependent_joins = []

        def find_dependent_joins(relalg: RelNode):
            if isinstance(node, DependentJoin):
                return dependent_joins.append(node)

            for child in node.children() or []:
                find_dependent_joins(child)

        find_dependent_joins(node)
        return dependent_joins
