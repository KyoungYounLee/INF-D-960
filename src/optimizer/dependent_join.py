from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from postbound.qal import predicates as preds
from postbound.qal.relalg import RelNode, VisitorResult, RelNodeVisitor


class DependentJoin(RelNode):
    def __init__(self, base_node: RelNode, dependent_node: RelNode, *,
                 predicate: Optional[preds.AbstractPredicate] = None,
                 parent_node: Optional[RelNode] = None) -> None:
        super().__init__(parent_node.mutate() if parent_node is not None else None)
        self._left_input = base_node
        self._right_input = dependent_node
        self._predicate = predicate
        self._hash_val = hash((self._left_input, self._right_input, self._predicate))
        self._maintain_child_links()

    @property
    def base_node(self) -> RelNode:
        return self._left_input

    @property
    def dependent_node(self) -> RelNode:
        return self._right_input

    @property
    def predicate(self) -> Optional[preds.AbstractPredicate]:
        return self._predicate

    def children(self) -> Sequence[RelNode]:
        return [self._left_input, self._right_input]

    def accept_visitor(self, visitor: RelNodeVisitor[VisitorResult]) -> VisitorResult:
        return visitor.visit_theta_join(self)

    def mutate(self, left_child: Optional[RelNode] = None, right_child: Optional[RelNode] = None, *,
               predicate: Optional[preds.AbstractPredicate] = None,
               parent: Optional[RelNode] = None, as_root: bool = False) -> DependentJoin:

        left_child = left_child.mutate(as_root=True) if left_child is not None else self._left_input.mutate(
            as_root=True)
        right_child = right_child.mutate(as_root=True) if right_child is not None else self._right_input.mutate(
            as_root=True)
        if as_root:
            parent = None
        else:
            # mutation of the parent is handled during the __init__ method of the current mutated node
            parent = parent if parent is not None else self._parent
        return DependentJoin(left_child, right_child, parent_node=parent)

    def __hash__(self) -> int:
        return self._hash_val

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, type(self))
                and self._left_input == other._left_input and self._right_input == other._right_input
                and self._predicate == other._predicate)

    def __str__(self) -> str:
        if self._predicate:
            return f"▶◁ ϴ=({self._predicate})"
        else:
            return f"▶◁"
