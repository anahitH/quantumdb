package io.quantumdb.demo.applications;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import io.quantumdb.demo.utils.ExecutionStats;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import io.quantumdb.demo.utils.DDL_TYPE;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class SchemaChangeApplication extends SchemaChange {

    public SchemaChangeApplication(String url, String server, String database, String username, String pass, String tableName) {
        super (url, server, database, username, pass, tableName);
    }

    @Override
    public List<ExecutionStats> runChange(DDL_TYPE ddlOp)
    {
        List<ExecutionStats> executionStats = new ArrayList<>(1);
        executionStats.add(runDDL(ddlOp));
        return executionStats;
    }

}
