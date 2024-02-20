from postbound.qal import parser, relalg
from postbound.qal.relalg import RelNode


class Parser:
    def __init__(self):
        pass

    def parse_relalg(self, sql_query: str) -> RelNode:
        parsed_query = parser.parse_query(sql_query)
        return relalg.parse_relalg(parsed_query)