from typing import Optional

from postbound.qal import base
from postbound.qal.relalg import RelNode, Projection, GroupBy, Selection, ThetaJoin, SemiJoin, AntiJoin, Map, Relation

from src.optimizer.dependent_join import DependentJoin
from src.utils.utils import Utils


class PushDownManager:
    def __init__(self, utils: Utils):
        self.utils = utils

    def push_down(self, node: Optional[DependentJoin]) -> RelNode:
        # 1. Navigieren zur Position des Knotens für den dependent-Join node
        dependent_join = self._navigate_to_dependent_join(node)

        # 2. Prüfen, ob das rechte Kind des dependent-join Knotens freie Variablen in der Baumstruktur hat
        dependent_node = dependent_join.right_input

        if self._check_free_variables_in_right_child(dependent_node):

            return True
        else:
            return False

        #  1) False
        #    - Die letzte Regel der Push-Down-Regel wird angewendet, der dependent-join-Knoten wird beseitigt und die Schleife wird beendet
        #  2) True
        #   - Überprüfen des Knotentyps des rechten_Kindes des dependent-join-Knotens
        #   - Anwenden eine Push-down-Regel für diesen Typ. An diesem Punkt wird die gesamte Baumstruktur aktualisiert
        #   - zurück zu Schritt 1 und wiederholen die Schleife

        return dependent_node

    def _navigate_to_dependent_join(self, node: RelNode) -> Optional[DependentJoin]:
        if isinstance(node, DependentJoin):
            return node

        for child in node.children():
            result = self._navigate_to_dependent_join(child)
            if result is not None:
                return result
        return None

    def _check_free_variables_in_right_child(self, node: RelNode) -> bool:
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
            if self._check_free_variables_in_right_child(child):
                return True

        return False

    def _apply_push_down_rule(self, node: RelNode):
        pass

    def _push_down_rule_selection(self):
        pass

    def _push_down_rule_projection(self):
        pass

    def _push_down_rule_groupby(self):
        pass

    def _push_down_rule_join(self):
        pass

    def _push_down_rule_final(self):
        pass
