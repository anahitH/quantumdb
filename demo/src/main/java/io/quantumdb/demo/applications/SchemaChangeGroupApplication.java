package io.quantumdb.demo.applications;

import io.quantumdb.demo.utils.DDL_TYPE;
import io.quantumdb.demo.utils.ExecutionStats;

import java.util.ArrayList;
import java.util.List;

public class SchemaChangeGroupApplication extends SchemaChange {

    private final int ddlNum;

    public SchemaChangeGroupApplication(String url, String server, String database, String username, String pass, String tableName, int ddlNum) {
        super (url, server, database, username, pass, tableName);
        this.ddlNum = ddlNum;
    }

    @Override
    public void prepareForDDLs(DDL_TYPE ddlOp) {
        for (int i = 0; i < this.ddlNum; ++i) {
            prepareForDDL(ddlOp);
        }
    }

    @Override
    public List<ExecutionStats> runChange(DDL_TYPE ddlOp) {
        List<ExecutionStats> ddlExecutions = new ArrayList<>(ddlNum);
        for (int i = 0; i < this.ddlNum; ++i) {
            ddlExecutions.add(0, runDDL(ddlOp));
        }
        return ddlExecutions;
    }
}
