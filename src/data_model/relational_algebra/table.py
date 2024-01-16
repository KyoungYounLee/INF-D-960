from postbound.qal.base import TableReference

from src.data_model.relational_algebra.node import Node


class Table(Node):
    def __init__(self, table: TableReference):
        super().__init__()
        self.table = table

    def __str__(self):
        return f"Table({self.table.full_name})"

    def __eq__(self, other):
        if isinstance(other, Table):
            return self.table.full_name == other.table.full_name
        return False

    def __hash__(self):
        return hash(self.table)

