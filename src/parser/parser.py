from postbound.qal import parser, relalg
from postbound.qal.relalg import RelNode


class Parser:
    def __init__(self):
        pass

    def parse_relalg(self, sql_query: str) -> RelNode:
        parsed_query = parser.parse_query(sql_query)
        return relalg.parse_relalg(parsed_query)

    def str_relalg(self, relalg: RelNode) -> str:
        def format_relalg(relalg: RelNode, level) -> str:
            node_str = f"{'    ' * level}{relalg.node_type}"
            for child in relalg.children():
                node_str += "\n" + format_relalg(child, level + 1)
            return node_str

        return format_relalg(relalg, 0)
