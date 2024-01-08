from src.data_model.relational_algebra.node import Node


class Join(Node):
    def __init__(self, left_child, right_child, condition, join_type="inner"):
        super().__init__(children=[left_child, right_child])
        self.condition = condition
        self.join_type = join_type

    def __str__(self):
        left, right = self.children
        return f"{self.join_type.capitalize()} Join({left}, {right}, On {self.condition})"