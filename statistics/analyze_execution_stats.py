import os
import json
from ddl_stats import ddl_stats

EXEC_STATS_DIR = "C:\\Users\\anhayrap\\Desktop\\quantumdb"
TEX_OUT_FOLDER = "C:\\Users\\anhayrap\\Desktop\\quantumdb\\statistics"
TEX_TABLES_FILE_NAME = "dblocking.tex"

# JSON keys
SELECTS = "selects"
INSERTS = "inserts"
UPDATES = "updates"
DELETES = "deletes"

NUM_OF_THREADS_RAN = "Num_of_threads_during_ddl"
DMLS_BEFORE_DDL = "DML_before_DDL"
DMLS_DURING_DDL = "DML_during_DDL"
DMLS_AFTER_DDL = "DML_after_DDL"
DDLS = "ddls"

ddls_execution_stats = {}


def get_stat_files(in_dir):
    return [name for name in os.listdir(in_dir) if name.endswith(".json")]


def parse_single_stat_file(stat_file_name):
    stat_file = os.path.join(EXEC_STATS_DIR, stat_file_name)
    if not os.path.isfile(stat_file):
        print("Unable to locate file " + stat_file_name + " in directory " + EXEC_STATS_DIR + ". The statistics will "
                                                                                              "be incomplete.")
        return
    stats = json.load(open(stat_file))
    ddl_op_name = ""
    for ddl_op_name_key in stats.keys():
        ddl_op_name = ddl_op_name_key
        break
    if ddl_op_name not in ddls_execution_stats:
        ddls_execution_stats[ddl_op_name] = ddl_stats(ddl_op_name)
    ddl_stat = ddls_execution_stats[ddl_op_name]
    for dml_name in stats[ddl_op_name].keys():
        ddl_stat.add_dml(dml_name)
        ddl_stat.set_num_of_unique_threads(dml_name, stats[ddl_op_name][dml_name][NUM_OF_THREADS_RAN])
        ddl_stat.add_ddl_execution_times(dml_name, stats[ddl_op_name][dml_name][DDLS])
        ddl_stat.add_dmls_before_ddl(dml_name, stats[ddl_op_name][dml_name][DMLS_BEFORE_DDL])
        ddl_stat.add_dmls_during_ddl(dml_name, stats[ddl_op_name][dml_name][DMLS_DURING_DDL])
        ddl_stat.add_dmls_after_ddl(dml_name, stats[ddl_op_name][dml_name][DMLS_AFTER_DDL])


def parse_data():
    stat_files = get_stat_files(EXEC_STATS_DIR)
    for stat_file in stat_files:
        parse_single_stat_file(stat_file)


def dump_table_for_ddl(ddl_stat):
    from tabulate import tabulate
    tabulate.LATEX_ESCAPE_RULES = {}
    table_headers = [ddl_stat.name, "ddl number", "ddl mean duration", "ddl median duration", "dml mean duration before ddls",
                     "dml median duration before ddls", "dml mean duration during ddls",
                     "dml median duration during ddls", "dml mean duration after ddls",
                     "dml median duration after ddls", "number of executed dml threads"]
    table_data = []
    for dml_name in ddl_stat.dmls:
        dml_row = [dml_name, len(ddl_stat.get_ddls(dml_name)), ddl_stat.get_ddl_mean_execution_time(dml_name),
                   ddl_stat.get_ddl_median_execution_time(dml_name),
                   ddl_stat.get_dml_mean_execution_time_before_ddl(dml_name),
                   ddl_stat.get_dml_median_execution_time_before_ddl(dml_name),
                   ddl_stat.get_dml_mean_execution_time_during_ddl(dml_name),
                   ddl_stat.get_dml_median_execution_time_during_ddl(dml_name),
                   ddl_stat.get_dml_mean_execution_time_after_ddl(dml_name),
                   ddl_stat.get_dml_median_execution_time_after_ddl(dml_name),
                   ddl_stat.get_num_of_unique_threads(dml_name)]
        table_data.append(dml_row)

    table_file = os.path.join(TEX_OUT_FOLDER, TEX_TABLES_FILE_NAME)
    table = tabulate(table_data, headers=table_headers, tablefmt="latex")
    with open(table_file, 'a+') as tablefile:
        tablefile.write(table)


def dump_tables():
    for ddl in ddls_execution_stats:
        dump_table_for_ddl(ddls_execution_stats[ddl])


def main():
    parse_data()
    print(ddls_execution_stats)
    dump_tables()


if __name__ == "__main__":
    main()
