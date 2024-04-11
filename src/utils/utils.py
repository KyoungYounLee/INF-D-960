class Utils:
    @staticmethod
    def detailed_structure_visualization(node, _indentation=0) -> str:
        padding = " " * _indentation
        prefix = f"{padding}<- " if padding else ""
        node_representation = f"{prefix}{node.__class__.__name__}"
        parent_representation = f"{padding}  Parent: {node.parent_node.__class__.__name__}" if node.parent_node else ""
        predicate_representation = f"{padding}  Predicate: {node.predicate}" if hasattr(node,
                                                                                        'predicate') and node.predicate else ""
        sideways_pass_representation = f"{padding}  Sideways Pass: {[n.__class__.__name__ for n in node.sideways_pass]}" if hasattr(
            node, 'sideways_pass') and node.sideways_pass else ""

        inspections = [node_representation, parent_representation, predicate_representation,
                       sideways_pass_representation]
        for child in node.children():
            inspections.append(Utils.detailed_structure_visualization(child, _indentation=_indentation + 2))
        return "\n".join(filter(None, inspections))
