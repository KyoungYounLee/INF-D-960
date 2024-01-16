from typing import Tuple
from src.data_model.relational_algebra.node import Node


class GroupBy(Node):
    def __init__(self, child, group_field):
        super().__init__([child])
        self.group_field = group_field

    def __str__(self):
        return f"GroupBy({self.group_field})"

