from postbound.qal.base import TableReference

from src.data_model.relational_algebra.node import Node


class Table(Node):
    def __init__(self, table: TableReference):
        super().__init__()
        self.name = table.full_name

    def __str__(self):
        return f"Table({self.name})"