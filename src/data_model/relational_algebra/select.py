from src.data_model.relational_algebra.node import Node


class Select(Node):
    def __init__(self, child, condition):
        super().__init__([child])
        self.condition = condition

    def __str__(self):
        return f"Select({self.condition})"
