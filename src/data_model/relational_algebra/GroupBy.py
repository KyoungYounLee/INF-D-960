from typing import Tuple

from src.data_model.relational_algebra.Node import Node


class GroupBy(Node):
    def __init__(self, child, group_field, aggregate_function: Tuple[str, str]):
        super().__init__([child])
        self.group_field = group_field

        # aggregate_function: Tuple[field_name, function_name]
        self.aggregate_function = aggregate_function

    def __str__(self):
        field, func = self.aggregate_function
        return f"GroupBy({str(self.children[0])}, {self.group_field}, {func}({field}))"

