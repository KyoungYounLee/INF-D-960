from typing import List

from postbound.qal.relalg import RelNode, SubqueryScan


class Optimizer:
    def __init__(self):
        pass

    def optimize_unnesting(self, relalg: RelNode) -> RelNode:
        """
        Checken ob die Query einen abhängigen Join hat.
        Abhängigen Join umwandeln
        Subquery (T2), Outerquery (T1) berechnen
        D berechnen
        Abhängigen Join in Unnesting-Form umwandeln
        """

        localRelNodes = self._find_dependent_subquery_node(relalg)
        if len(localRelNodes) > 0:
            return localRelNodes[0]

        return relalg

    def _find_dependent_subquery_node(self, relalg: RelNode) -> List[RelNode]:
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

    def _transform_dependent_join(self):
        pass

    def _derive_sub_and_outer_queries(self):
        pass

    def _derive_domain_D(self):
        pass

    def _convert_to_unnesting(self):
        pass
