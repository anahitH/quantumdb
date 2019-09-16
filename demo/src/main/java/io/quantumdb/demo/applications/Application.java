package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.quantumdb.demo.utils.ExecutionStats;
import io.quantumdb.demo.utils.PerformanceTracker;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class Application {


    protected static final int TABLE_SIZE = 10_000_000;
    private static final int THREADS = 100;

    private final String url;
    private final String server;
    private final String database;
    private final String user;
    private final String pass;
    private final String tableName;
    private final PerformanceTracker.Type currentDml;

    private final PerformanceTracker tracker = new PerformanceTracker();

    private ScheduledThreadPoolExecutor executorService;
    private Connection connection;

    public void run() throws SQLException {
        //tracker.reset();
        connection = getConnection();
        executorService = new ScheduledThreadPoolExecutor(THREADS);
        for (int i = 0; i < THREADS; i++) {
            executorService.scheduleWithFixedDelay(createDatabaseInteractor(connection, i), 5, 10, TimeUnit.NANOSECONDS);
            //executorService.schedule(createDatabaseInteractor(connection), 0, TimeUnit.NANOSECONDS);
        }
    }


    public void stop() throws InterruptedException, SQLException {
        if (executorService != null) {
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    public String getPerformance() {
        synchronized (tracker) {
            String output = tracker.generateSimplifiedOutput();
            //tracker.reset();
            return output;
        }
    }

    public String getFullPerformanceReport()
    {
        synchronized (tracker) {
            String output = tracker.generateOutput();
            //tracker.reset();
            return output;
        }
    }
    abstract ExecutionStats performInsert(Connection conn) throws SQLException;

    abstract ExecutionStats performSelect(Connection conn) throws SQLException;

    abstract ExecutionStats performUpdate(Connection conn) throws SQLException;

    abstract ExecutionStats performDelete(Connection conn) throws SQLException;

    Runnable createDatabaseInteractor(Connection connection, int threadNum) {
        return () -> {
            try {
                ExecutionStats execStats = null;
                switch (currentDml)
                {
                    case INSERT:
                        execStats = performInsert(connection);
                        break;
                    case SELECT:
                        execStats = performSelect(connection);
                        break;
                    case DELETE:
                        execStats = performDelete(connection);
                        break;
                    case UPDATE:
                        execStats = performUpdate(connection);
                        break;
                }
                if (execStats != null) {
                    execStats.setThreadNum(threadNum);
                    synchronized (tracker) {
                        tracker.registerDuration(this.currentDml, execStats);
                    }
                }
                //PerformanceTracker.ExecutionStats execStats = performSelect(connection);
                //PerformanceTracker.ExecutionStats execStats = performUpdate(connection);
                //PerformanceTracker.ExecutionStats execStats = performDelete(connection);
                    //tracker.registerDuration(PerformanceTracker.Type.SELECT, execStats);
                    //tracker.registerDuration(PerformanceTracker.Type.UPDATE, execStats);
                    //tracker.registerDuration(PerformanceTracker.Type.DELETE, execStats);
            }
            catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        };
    }

    private Connection getConnection()
    {
        Properties props = new Properties();
        props.setProperty("serverName", server);
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("databaseName", database);
        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e)
        {
            System.out.println("Failed to create connection");
        }
        return null;
    }

    public void savePerformanceStatistics(String ddl, List<ExecutionStats> ddlExecutionStats) {
        tracker.savePerformanceStatistics(ddl, ddlExecutionStats);
    }
}
