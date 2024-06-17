import argparse
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

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


def main(mode, sql_directory):
    connect_string = f"postgresql://postgres:1234@localhost:5432/kp"
    parser = Parser()
    results = []

    # Setup
    pg_instance = postgres.connect(connect_string=connect_string, cache_enabled=False)
    postgres_interface = postgres.PostgresInterface(connect_string=connect_string)
    queries = load_sql_files(sql_directory)

    if mode == 'normal':
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
                    results.append(
                        (query_name, original_execution_time, optimized_execution_time, original_result, None))
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

    else:
        log_output = []

        for query_name, query in queries:
            relalg_query = parser.parse_relalg(query)

            postgres_interface.prewarm_tables(relalg_query.tables())
            # plan = pg_instance.optimizer().analyze_plan(query)
            plan = pg_instance.optimizer().query_plan(query)
            print(f"original query {query_name}: ")
            print(plan.inspect())
            log_output.append(f"original query {query_name}: ")
            log_output.append(plan.inspect())

            print(query_name)
            optimized_query = optimize_subquery(relalg_query)
            postgres_interface.prewarm_tables(relalg_query.tables())
            optimized_plan = pg_instance.optimizer().analyze_plan(optimized_query)
            print(f"optimized query {query_name}: ")
            print(optimized_plan.inspect())
            log_output.append(f"optimized query {query_name}: ")
            log_output.append(optimized_plan.inspect())

        with open("output/query_analysis_logs.txt", "w") as log_file:
            log_file.write("\n".join(log_output))

    return pg_instance


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some queries.')
    parser.add_argument('--mode', type=str, choices=['normal', 'analysis'], default='normal',
                        help='Mode to run the script in. Can be "normal" or "analysis".')

    parser.add_argument('--sql_directory', type=str, default='benchmark_queries')

    args = parser.parse_args()
    main(args.mode, args.sql_directory)
