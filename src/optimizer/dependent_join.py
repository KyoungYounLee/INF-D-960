from __future__ import annotations

from collections.abc import Sequence
from typing import Optional

from postbound.qal import predicates as preds
from postbound.qal.relalg import RelNode, VisitorResult, RelNodeVisitor


class DependentJoin(RelNode):
    def __init__(self, *, base_node: RelNode, dependent_node: RelNode,
                 predicate: Optional[preds.AbstractPredicate] = None,
                 parent_node: Optional[RelNode] = None) -> None:
        super().__init__(parent_node.mutate() if parent_node is not None else None)
        self._base_node = base_node
        self._dependent_node = dependent_node
        self._predicate = predicate
        self._hash_val = hash((self._base_node, self._dependent_node, self._predicate))
        self._maintain_child_links()

    @property
    def base_node(self) -> RelNode:
        return self._base_node

    @property
    def dependent_node(self) -> RelNode:
        return self._dependent_node

    @property
    def predicate(self) -> Optional[preds.AbstractPredicate]:
        return self._predicate

    def children(self) -> Sequence[RelNode]:
        return [self._base_node, self._dependent_node]

    def accept_visitor(self, visitor: RelNodeVisitor[VisitorResult]) -> VisitorResult:
        return visitor.visit_theta_join(self)

    def mutate(self, *, left_child: Optional[RelNode] = None, right_child: Optional[RelNode] = None,
               predicate: Optional[preds.AbstractPredicate] = None,
               parent: Optional[RelNode] = None, as_root: bool = False) -> DependentJoin:

        base_node = left_child if left_child is not None else self._base_node
        dependent_node = right_child if right_child is not None else self._dependent_node
        if as_root:
            parent = None
        else:
            # mutation of the parent is handled during the __init__ method of the current mutated node
            parent = parent if parent is not None else self._parent
        return DependentJoin(base_node=base_node, dependent_node=dependent_node, predicate=predicate,
                             parent_node=parent)

    def __hash__(self) -> int:
        return self._hash_val

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, type(self))
                and self._base_node == other._base_node and self._dependent_node == other._dependent_node
                and self._predicate == other._predicate)

    def __str__(self) -> str:
        if self._predicate:
            return f"▶◁ ϴ=({self._predicate})"
        else:
            return f"▶◁"
