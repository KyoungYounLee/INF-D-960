from postbound.qal.relalg import RelNode, ThetaJoin, CrossProduct, AntiJoin, SemiJoin

from src.optimizer.dependent_join import DependentJoin


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

    def update_relalg_structure_upward(self, node: RelNode, **kwargs) -> RelNode:
        """
            Recursively updates the relational algebra structure from a given node upwards to its ancestors.
            This method is used to propagate changes from a node to all its parent nodes in the tree,
            ensuring that all relevant parts of the tree reflect the changes made to the node or its siblings.

            Parameters:
            - node: The node from which the updates will start.
            - **kwargs: Parameters to be used for mutating the node, such as changing specific attributes.

            Returns:
            - The topmost updated node after all recursive updates.
        """
        updated_node = node.mutate(**kwargs)

        if updated_node.parent_node is None:
            return updated_node

        parent_node = updated_node.parent_node
        if parent_node is None:
            return updated_node
        elif isinstance(parent_node, (ThetaJoin, CrossProduct, DependentJoin)):
            if parent_node.left_input == node:
                updated_parent_node = self.update_relalg_structure_upward(parent_node, left_child=updated_node)
            else:
                updated_parent_node = self.update_relalg_structure_upward(parent_node, right_child=updated_node)
        elif isinstance(parent_node, (AntiJoin, SemiJoin)):
            if parent_node.input_node == node:
                updated_parent_node = self.update_relalg_structure_upward(parent_node, input_node=updated_node)
            else:
                updated_parent_node = self.update_relalg_structure_upward(parent_node, subquery_node=updated_node)
        else:
            updated_parent_node = self.update_relalg_structure_upward(parent_node, input_node=updated_node)

        return next((child for child in updated_parent_node.children() if child == updated_node), updated_node)
