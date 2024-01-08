from abc import ABC, abstractmethod


class Node(ABC):
    def __init__(self, children=None):
        self.children = children if children is not None else []

    @abstractmethod
    def __str__(self):
        pass
