import numpy_utils


class dml_stats:
    def __init__(self, name):
        self.name = name;
        self.num_of_unique_threads = 0
        self.ddls = []
        self.before_ddl = []
        self.during_ddl = []
        self.after_ddl = []

    def set_num_of_unique_threads(self, num):
        self.num_of_unique_threads = num

    def add_ddls(self, ddl_ex_times):
        self.ddls.extend(ddl_ex_times)

    def add_before_ddls(self, ex_times):
        self.before_ddl.extend(ex_times)

    def add_during_ddls(self, ex_times):
        self.during_ddl.extend(ex_times)

    def add_after_ddls(self, ex_times):
        self.after_ddl.extend(ex_times)


class ddl_stats:
    def __init__(self, name):
        self.name = name
        self.dmls = []
        self.dml_exec_stats = {}

    def add_dml(self, dml_name):
        if dml_name not in self.dmls:
            self.dmls.append(dml_name)
        if dml_name not in self.dml_exec_stats:
            self.dml_exec_stats[dml_name] = dml_stats(dml_name)

    def set_num_of_unique_threads(self, dml_name, num):
        self.dml_exec_stats[dml_name].set_num_of_unique_threads(num)

    def get_num_of_unique_threads(self, dml_name):
        return self.dml_exec_stats[dml_name].num_of_unique_threads

    def add_ddl_execution_times(self, dml_name, ex_times):
        self.dml_exec_stats[dml_name].add_ddls(ex_times)

    def get_ddls(self, dml_name):
        return self.dml_exec_stats[dml_name].ddls;

    def add_dmls_before_ddl(self, dml_name, ex_times):
        self.dml_exec_stats[dml_name].add_before_ddls(ex_times)

    def add_dmls_during_ddl(self, dml_name, ex_times):
        self.dml_exec_stats[dml_name].add_during_ddls(ex_times)

    def add_dmls_after_ddl(self, dml_name, ex_times):
        self.dml_exec_stats[dml_name].add_after_ddls(ex_times)

    def get_ddl_mean_execution_time(self, dml_name):
        return numpy_utils.average(self.dml_exec_stats[dml_name].ddls)

    def get_ddl_median_execution_time(self, dml_name):
        return numpy_utils.median(self.dml_exec_stats[dml_name].ddls)

    def get_dml_mean_execution_time_before_ddl(self, dml_name):
        return numpy_utils.average(self.dml_exec_stats[dml_name].before_ddl)

    def get_dml_median_execution_time_before_ddl(self, dml_name):
        return numpy_utils.median(self.dml_exec_stats[dml_name].before_ddl)

    def get_dml_mean_execution_time_during_ddl(self, dml_name):
        return numpy_utils.average(self.dml_exec_stats[dml_name].during_ddl)

    def get_dml_median_execution_time_during_ddl(self, dml_name):
        return numpy_utils.median(self.dml_exec_stats[dml_name].during_ddl)

    def get_dml_mean_execution_time_after_ddl(self, dml_name):
        return numpy_utils.average(self.dml_exec_stats[dml_name].after_ddl)

    def get_dml_median_execution_time_after_ddl(self, dml_name):
        return numpy_utils.median(self.dml_exec_stats[dml_name].after_ddl)
