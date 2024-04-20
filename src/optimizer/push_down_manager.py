from typing import List, Optional

from postbound.qal.base import ColumnReference
from postbound.qal.relalg import RelNode

from src.optimizer.dependent_join import DependentJoin
from src.utils.utils import Utils


class PushDownManager:
    def __init__(self, utils: Utils):
        self.utils = utils

    def push_down(self, node: RelNode, free_variables: List[ColumnReference]) -> RelNode:
        # 1. Navigieren zur Position des Knotens für den dependent-Join node
        dependent_node = self._navigate_to_dependent_join(node)

        # 2. Prüfen, ob das rechte Kind des dependent-join Knotens freie Variablen in der Baumstruktur hat
        #  1) False
        #    - Die letzte Regel der Push-Down-Regel wird angewendet, der dependent-join-Knoten wird beseitigt und die Schleife wird beendet
        #  2) True
        #   - Überprüfen des Knotentyps des rechten_Kindes des dependent-join-Knotens
        #   - Anwenden eine Push-down-Regel für diesen Typ. An diesem Punkt wird die gesamte Baumstruktur aktualisiert
        #   - zurück zu Schritt 1 und wiederholen die Schleife

        return dependent_node

    def _navigate_to_dependent_join(self, node: RelNode) -> Optional[RelNode]:
        if isinstance(node, DependentJoin):
            return node

        for child in node.children():
            result = self._navigate_to_dependent_join(child)
            if result is not None:
                return result
        return None

    def _check_free_variables_in_right_child(self):
        pass

    def _apply_push_down_rule(self):
        pass
