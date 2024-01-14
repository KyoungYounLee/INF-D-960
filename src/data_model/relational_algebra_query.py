from src.data_model.relational_algebra.node import Node


class RelationalAlgebraQuery:
    """
    Represents a Relational Algebra query as a tree structure. The tree structure is ideal for representing
    complex queries comprising multiple relational algebra operations, such as selections, joins, and projections.

    In this tree structure:
    - Each node represents a single relational algebra operation.
    - The root node (`self.root`) is the starting point of the query, representing the top-level operation.
    - Child nodes represent sub-operations, forming the branches of the tree.

    This approach allows for efficient traversal and manipulation of the query structure, enabling
    operations like query optimization and transformation to be implemented more effectively.

    Attributes:
        root (Node): The root node of the query tree, representing the top-level relational algebra operation.
    """

    def __init__(self, root_operation: Node):
        self.root = root_operation

    def __str__(self):
        return self._format_node(self.root, level=0)

    def _format_node(self, node, level):
        node_str = f"{'    ' * level}{node}"

        for child in node.children:
            node_str += "\n" + self._format_node(child, level + 1)

        return node_str

