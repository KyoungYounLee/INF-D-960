from src.data_model.relational_algebra.Node import Node


class Project(Node):
    def __init__(self, child, columns):
        super().__init__([child])
        self.columns = columns

    def __str__(self):
        columns_str = ', '.join(self.columns)
        return f"Project({str(self.children[0])}, Columns [{columns_str}])"