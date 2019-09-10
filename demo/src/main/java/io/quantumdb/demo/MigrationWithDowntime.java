package io.quantumdb.demo;

import static java.lang.Thread.sleep;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.quantumdb.demo.applications.CurrentApplication;
import io.quantumdb.demo.applications.SchemaChangeApplication;
import io.quantumdb.demo.applications.SchemaChangeGroupApplication;
import io.quantumdb.demo.utils.ExecutionStats;
import io.quantumdb.demo.utils.PerformanceTracker;
import io.quantumdb.demo.utils.DDL_TYPE;
import io.quantumdb.demo.utils.TableConstants;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class MigrationWithDowntime extends Migration {

    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException {
        String url = System.getProperty("url");
        String server = System.getProperty("server");
        String user = System.getProperty("user");
        String pass = System.getProperty("pass");
        String database = System.getProperty("database");
        String tableName = System.getProperty("table");

        Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "You must specify a VM argument '-Durl'.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(user), "You must specify a VM argument '-Duser'.");

        new MigrationWithDowntime(url, server, user, pass, database, tableName).run();
    }

    public MigrationWithDowntime(String url, String server, String user, String pass, String database, String tableName) {
        super(url, server, user, pass, database, tableName);
    }

    public void run() throws InterruptedException, ClassNotFoundException, SQLException {
        Connection connection = createConnection();

        log.info("Setting up database for demo...");
        System.out.println("Setting up database for demo...");
        //createUserTable(connection);
        //fillUserTable();
        //createBackupUserTable(connection);

        //performDmls(connection);
        performSchemaChange(connection, PerformanceTracker.Type.SELECT);
        //performSchemaChange(connection, PerformanceTracker.Type.INSERT);
        //performSchemaChange(connection, PerformanceTracker.Type.UPDATE);
        //performSchemaChange(connection, PerformanceTracker.Type.DELETE);
    }


    @Override
    protected void performSchemaChange(Connection connection, PerformanceTracker.Type dmlType) throws InterruptedException, SQLException {
        //SchemaChangeApplication schemaChange = new SchemaChangeApplication(getUrl(), getServer(), getDatabase(), getUser(), getPass(), this.getTableName());
        SchemaChangeGroupApplication schemaChange = new SchemaChangeGroupApplication(getUrl(), getServer(), getDatabase(), getUser(), getPass(), this.getTableName(), 3);
        for (int i = DDL_TYPE.ADD_COLUMN.getValue(); i < DDL_TYPE.DROP_COLUMN.getValue(); ++i) {
            DDL_TYPE ddlOp = DDL_TYPE.getDDLOp(i);
            System.out.println("Prepare for DDL " + DDL_TYPE.getDDLName(ddlOp) + " ...");
            schemaChange.prepareForDDL(ddlOp);
            //sleep(5_000);
            log.info("Starting the application (version 1)...");
            System.out.println("Starting the application (version 1)...");
            CurrentApplication current = new CurrentApplication(getUrl(), getServer(), getDatabase(), getUser(), getPass(), this.getTableName(), dmlType);
            current.run();

            sleep(100_000);
            //log.info("Application (version 1) performance before schema change: \n\n" + current.getPerformance());
            log.info("Application (version 1) performance before schema change: \n\n" + current.getFullPerformanceReport());
            //System.out.println("Application (version 1) performance before schema change: \n\n" + current.getPerformance());

            //sleep(2_000);
            log.info("Performing a schema change " + DDL_TYPE.getDDLName(ddlOp));
            System.out.println("Performing a schema change " + DDL_TYPE.getDDLName(ddlOp));
            long start = System.currentTimeMillis();
            List<ExecutionStats> executionStats = schemaChange.runChange(ddlOp);
            long end = System.currentTimeMillis();
            //if (executionStats != null) {
            //    log.info("Schema modification time: " + executionStats.toString());
            //}
            log.info("  Schema was modified, took: {}ms", (end - start));

            sleep(100_000);
            //sleep(5_000);
            //log.info("Application (version 1) performance during schema change: \n\n" + current.getPerformance());
            log.info("Application (version 1) performance during schema change: \n\n" + current.getFullPerformanceReport());
            //System.out.println("Application (version 1) performance during schema change: \n\n" + current.getPerformance());

            log.info("Stop the application (version 1)...");
            System.out.println("Stop the application (version 1)...");
            current.stop();

            current.savePerformanceStatistics(DDL_TYPE.getDDLName(ddlOp), executionStats);
            TableConstants.resetUniqueNameIdGenerator();
        }
    }

    private void performDmls(Connection connection) throws SQLException {
        CurrentApplication current = new CurrentApplication(getUrl(), getServer(), getDatabase(), getUser(), getPass(), this.getTableName(), PerformanceTracker.Type.SELECT);
        current.run();
        log.info("Application (version 1) performance during schema change: \n\n" + current.getFullPerformanceReport());

    }

}
