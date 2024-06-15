import os
import time
from typing import List, Tuple, Any

import pandas as pd
from postbound.db import postgres
from postbound.qal import qal
from postbound.qal.relalg import RelNode

from src.optimizer.optimizer import Optimizer
from src.optimizer.push_down_manager import PushDownManager
from src.parser.parser import Parser
from src.query_generator.query_generator import QueryGenerator
from src.utils.utils import Utils


def optimize_subquery(relalg_query: RelNode) -> qal.SqlQuery:
    utils = Utils()
    optimizer = Optimizer(utils)

    optimized_result = optimizer.optimize_unnesting(relalg_query)
    push_down_manager = PushDownManager(utils)
    query_generator = QueryGenerator(utils)

    push_down, subquery_root = push_down_manager.push_down(optimized_result)
    final_query = query_generator.generate_sql_from_relalg(push_down, subquery_root)

    return final_query


def load_sql_files(directory: str) -> List[Tuple[str, str]]:
    sql_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.sql')]
    queries = []
    for file_path in sql_files:
        with open(file_path, 'r') as file:
            queries.append((os.path.basename(file_path), file.read().strip()))
    return queries


def execute_query_and_measure_time(postgres_interface: postgres.PostgresInterface, query: qal.SqlQuery | str) -> Tuple[
    Any, float]:
    start_time = time.time()
    result = postgres_interface.execute_query(query)
    execution_time = time.time() - start_time
    return result, execution_time


def convert_to_strings(data):
    if isinstance(data, (list, tuple)):
        return [str(item) for item in data]
    else:
        return str(data)


def main():
    connect_string = f"postgresql://postgres:1234@localhost:5432/kp"
    sql_directory = 'benchmark_queries'
    parser = Parser()
    results = []

    # Setup
    postgres_db = postgres.connect(connect_string=connect_string, cache_enabled=False)
    postgres_interface = postgres.PostgresInterface(connect_string=connect_string)
    queries = load_sql_files(sql_directory)

    for query_name, query in queries:
        relalg_query = parser.parse_relalg(query)
        postgres_interface.prewarm_tables(relalg_query.tables())
        original_result, original_execution_time = execute_query_and_measure_time(postgres_interface, query)
        print(f"Query {query_name}: {original_result}")
        print(f"Original: " + str(original_execution_time))

        optimized_query = optimize_subquery(relalg_query)
        postgres_interface.prewarm_tables(relalg_query.tables())

        try:
            optimized_result, optimized_execution_time = execute_query_and_measure_time(postgres_interface,
                                                                                        str(optimized_query))
            print(f"Optimized: " + str(optimized_execution_time))

            if convert_to_strings(original_result) == convert_to_strings(optimized_result):
                results.append((query_name, original_execution_time, optimized_execution_time, original_result, None))
            else:
                print(f"Results: " + str(original_result) + " ," + str(optimized_result))
                error_message = "Optimized results differ from original results"
                results.append((query_name, original_execution_time, None, original_result, error_message))

        except Exception as e:
            error_message = str(e)
            print(f"Error: {error_message}")
            results.append((query_name, original_execution_time, None, original_result, error_message))
            continue

    df = pd.DataFrame(results,
                      columns=["Query Name", "Original Execution Time", "Optimized Execution Time", "Query Result",
                               "Error"])
    df.to_csv("output/query_execution_times.csv", index=False)

    return postgres_db


if __name__ == "__main__":
    main()
