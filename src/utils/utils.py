from typing import List

from postbound.qal.base import ColumnReference, TableReference
from postbound.qal.relalg import RelNode, ThetaJoin, AntiJoin, SemiJoin, Map, Projection, GroupBy, \
    Selection, Rename

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

    def find_all_dependent_columns(self, base_node: RelNode, dependent_node: RelNode) -> List[ColumnReference]:
        """
        Finds all columns that are dependent between the given base node and dependent node.
        """

        if dependent_node == base_node:
            return []

        tables = base_node.tables()
        dependent_columns = []

        if isinstance(dependent_node, (Map, Projection, GroupBy)):
            dependent_columns += self._extract_columns_from_simple_conditions(dependent_node, tables)
        elif isinstance(dependent_node, (Selection, ThetaJoin, DependentJoin, SemiJoin, AntiJoin)):
            dependent_columns += self._extract_columns_from_composite_conditions(dependent_node, tables)

        for child_node in dependent_node.children():
            dependent_columns += self.find_all_dependent_columns(base_node, child_node)

        dependent_columns = list(set(dependent_columns))
        return dependent_columns

    @staticmethod
    def _extract_columns_from_composite_conditions(node: Selection | ThetaJoin | DependentJoin | SemiJoin | AntiJoin,
                                                   tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]
        if node.predicate:
            for column in node.predicate.itercolumns():
                if column.table.identifier() in tables_identifier:
                    columns.append(column)
        return columns

    @staticmethod
    def _extract_columns_from_simple_conditions(node: Rename | Map | GroupBy | Projection,
                                                tables: frozenset[TableReference]) -> List[ColumnReference]:
        columns = []
        tables_identifier = [table.identifier() for table in tables]

        if isinstance(node, (GroupBy, Projection)):
            node_columns = node.group_columns if isinstance(node, GroupBy) else node.columns

            for sql_expr in node_columns:
                for column in sql_expr.itercolumns():
                    if column.table.identifier() in tables_identifier:
                        columns.append(column)

        if isinstance(node, (GroupBy, Map, Rename)):
            disc = node.aggregates if isinstance(node, GroupBy) else node.mapping
            for key_set, value_set in disc.items():
                for key_expr in key_set:
                    for key_column in key_expr.itercolumns():
                        if key_column.table.identifier() in tables_identifier:
                            columns.append(key_column)

                for value_expr in value_set:
                    for value_column in value_expr.itercolumns():
                        if value_column.table.identifier() in tables_identifier:
                            columns.append(value_column)

        return columns
