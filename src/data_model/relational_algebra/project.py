from postbound.qal.base import ColumnReference

from src.data_model.relational_algebra.node import Node


class Project(Node):
    def __init__(self, child, columns: set[ColumnReference]):
        super().__init__([child])
        self.columns = columns

    def __str__(self):
        columns_str = ', '.join([str(column.table) + '.' + str(column.name) for column in self.columns])
        return f"Project({columns_str})"
