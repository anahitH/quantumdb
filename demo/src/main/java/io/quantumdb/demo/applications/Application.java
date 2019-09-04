package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.quantumdb.demo.utils.PerformanceTracker;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class Application {


    protected static final int TABLE_SIZE = 10_000_000;
    private static final int THREADS = 8;

    private final String url;
    private final String server;
    private final String database;
    private final String user;
    private final String pass;

    private final PerformanceTracker tracker = new PerformanceTracker();

    private ScheduledThreadPoolExecutor executorService;
    private Connection connection;

    public void run() throws SQLException {
        tracker.reset();
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
            tracker.reset();
            return output;
        }
    }

    public String getFullPerformanceReport()
    {
        synchronized (tracker) {
            String output = tracker.generateOutput();
            tracker.reset();
            return output;
        }
    }
    abstract PerformanceTracker.ExecutionStats performInsert(Connection conn) throws SQLException;

    abstract PerformanceTracker.ExecutionStats performSelect(Connection conn) throws SQLException;

    abstract PerformanceTracker.ExecutionStats performUpdate(Connection conn) throws SQLException;

    abstract PerformanceTracker.ExecutionStats performDelete(Connection conn) throws SQLException;

    Runnable createDatabaseInteractor(Connection connection, int threadNum) {
        return () -> {
            try {
                PerformanceTracker.ExecutionStats execStats = performInsert(connection);
                //PerformanceTracker.ExecutionStats execStats = performSelect(connection);
                //PerformanceTracker.ExecutionStats execStats = performUpdate(connection);
                //PerformanceTracker.ExecutionStats execStats = performDelete(connection);
                execStats.setThreadNum(threadNum);
                synchronized (tracker) {
                    tracker.registerDuration(PerformanceTracker.Type.INSERT, execStats);
                    //tracker.registerDuration(PerformanceTracker.Type.SELECT, execStats);
                    //tracker.registerDuration(PerformanceTracker.Type.UPDATE, execStats);
                    //tracker.registerDuration(PerformanceTracker.Type.DELETE, execStats);
                }
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

    public void savePerformanceAsCSV(String filePrefix)
    {
        tracker.saveOutputToCSV(filePrefix);
    }

    public void savePerformanceStatistics(String ddl, PerformanceTracker.ExecutionStats ddlExecutionStats) {
        tracker.savePerformanceStatistics(ddl, ddlExecutionStats);
    }
}
