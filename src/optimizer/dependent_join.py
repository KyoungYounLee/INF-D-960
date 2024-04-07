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
        self._left_child = base_node
        self._right_child = dependent_node
        self._predicate = predicate
        self._hash_val = hash((self._left_child, self._right_child, self._predicate))
        self._maintain_child_links()

    @property
    def base_node(self) -> RelNode:
        return self._left_child

    @property
    def dependent_node(self) -> RelNode:
        return self._right_child

    @property
    def predicate(self) -> Optional[preds.AbstractPredicate]:
        return self._predicate

    def children(self) -> Sequence[RelNode]:
        return [self._left_child, self._right_child]

    def accept_visitor(self, visitor: RelNodeVisitor[VisitorResult]) -> VisitorResult:
        return visitor.visit_theta_join(self)

    def mutate(self, left_child: Optional[RelNode] = None, right_child: Optional[RelNode] = None, *,
               predicate: Optional[preds.AbstractPredicate] = None,
               parent: Optional[RelNode] = None, as_root: bool = False) -> DependentJoin:

        if left_child is not None:
            self._left_child = left_child
            self._left_child.mutate(parent=self)
        if right_child is not None:
            self._right_child = right_child
            self._right_child.mutate(parent=self)

        if not as_root and parent is not None:
            self._parent = parent

        if as_root:
            self._parent = None

        return self

    def __hash__(self) -> int:
        return self._hash_val

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, type(self))
                and self._left_child == other._left_child and self._right_child == other._right_child
                and self._predicate == other._predicate)

    def __str__(self) -> str:
        if self._predicate:
            return f"▶◁ ϴ=({self._predicate})"
        else:
            return f"▶◁"
