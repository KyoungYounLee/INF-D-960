from src.data_model.relational_algebra.Node import Node


class Table(Node):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def __str__(self):
        return f"Table({self.name})"