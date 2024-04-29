from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from postbound.qal import predicates as preds
from postbound.qal.relalg import RelNode, VisitorResult, RelNodeVisitor


class DependentJoin(RelNode):
    def __init__(self, base_node: RelNode, dependent_node: RelNode, *,
                 predicate: Optional[preds.AbstractPredicate] = None,
                 parent_node: Optional[RelNode] = None) -> None:
        self._left_input = base_node
        self._right_input = dependent_node
        self._predicate = predicate
        super().__init__(parent_node)

    @property
    def left_input(self) -> RelNode:
        return self._left_input

    @property
    def right_input(self) -> RelNode:
        return self._right_input

    @property
    def predicate(self) -> Optional[preds.AbstractPredicate]:
        return self._predicate

    def children(self) -> Sequence[RelNode]:
        return [self._left_input, self._right_input]

    def accept_visitor(self, visitor: RelNodeVisitor[VisitorResult]) -> VisitorResult:
        return visitor.visit_theta_join(self)

    def mutate(self, *, left_input: Optional[RelNode] = None, right_input: Optional[RelNode] = None,
               predicate: Optional[preds.AbstractPredicate] = None, as_root: bool = False) -> DependentJoin:
        params = {param: val for param, val in locals().items() if param != "self" and not param.startswith("__")}
        return super().mutate(**params)

    def _recalc_hash_val(self) -> int:
        return hash((self._left_input, self._right_input, self._predicate))

    __hash__ = RelNode.__hash__

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, type(self))
                and self._left_input == other._left_input and self._right_input == other._right_input
                and self._predicate == other._predicate)

    def __str__(self) -> str:
        if self._predicate:
            return f"▶◁ ϴ=({self._predicate})"
        else:
            return f"▶◁"
