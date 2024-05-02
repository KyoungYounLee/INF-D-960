import unittest

from postbound.qal import base, expressions, predicates, relalg

from src.optimizer.optimizer import Optimizer


class OptimizerTest(unittest.TestCase):

    def verify_parent_child_consistency(self, node: relalg.RelNode):
        children = node.children()
        for child in children:
            if child.parent_node != node:
                return False
            if not self.verify_parent_child_consistency(child):
                return False
        return True

    def test_update_relalg_structure(self):
        tab_s = base.TableReference("S")
        col_s_a = base.ColumnReference("a", tab_s)
        col_s_b = base.ColumnReference("b", tab_s)
        scan_s = relalg.Relation(base.TableReference("S"), [col_s_a, col_s_b])
        select_s = relalg.Selection(scan_s, predicates.as_predicate(col_s_a, expressions.LogicalSqlOperators.Equal, 42))

        tab_r = base.TableReference("R")
        col_r_a = base.ColumnReference("a", tab_r)
        scan_r = relalg.Relation(base.TableReference("R"), [col_r_a])

        join_node = relalg.ThetaJoin(select_s, scan_r,
                                     predicates.as_predicate(col_s_b, expressions.LogicalSqlOperators.Equal, col_r_a))

        additional_selection = relalg.Selection(join_node,
                                                predicates.as_predicate(col_s_b, expressions.LogicalSqlOperators.Equal,
                                                                        24))

        optimizer = Optimizer()
        updated_additional_selection = optimizer._update_relalg_structure(additional_selection, input_node=join_node)
        self.assertTrue(self.verify_parent_child_consistency(updated_additional_selection))


if __name__ == '__main__':
    unittest.main()
