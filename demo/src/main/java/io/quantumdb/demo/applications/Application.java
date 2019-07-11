package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.quantumdb.demo.utils.PerformanceTracker;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class Application {


    protected static final int TABLE_SIZE = 10_000_000;
    private static final int THREADS = 2;

    private final String url;
    private final String user;
    private final String pass;

    private final PerformanceTracker tracker = new PerformanceTracker();

    private ScheduledThreadPoolExecutor executorService;
    private Connection connection;

    public void run() throws SQLException {
        tracker.reset();
        connection = DriverManager.getConnection(url, user, pass);
        executorService = new ScheduledThreadPoolExecutor(THREADS);
        for (int i = 0; i < THREADS; i++) {
            executorService.scheduleWithFixedDelay(createDatabaseInteractor(connection), 100, 100, TimeUnit.MILLISECONDS);
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

    abstract int performInsert(Connection conn) throws SQLException;

    abstract int performSelect(Connection conn) throws SQLException;

    abstract int performUpdate(Connection conn) throws SQLException;

    abstract int performDelete(Connection conn) throws SQLException;

    Runnable createDatabaseInteractor(Connection connection) {
        Random random = new Random();
        return () -> {
            try {
                int i = random.nextInt(4);
                if (i == 0) {
                    int duration = performInsert(connection);
                    synchronized (tracker) {
                        tracker.registerDuration(PerformanceTracker.Type.INSERT, duration);
                    }
                }
                else if (i == 1) {
                    int duration = performSelect(connection);
                    synchronized (tracker) {
                        tracker.registerDuration(PerformanceTracker.Type.SELECT, duration);
                    }
                }
                else if (i == 2) {
                    int duration = performUpdate(connection);
                    synchronized (tracker) {
                        tracker.registerDuration(PerformanceTracker.Type.UPDATE, duration);
                    }
                }
                else if (i == 3) {
                    int duration = performDelete(connection);
                    synchronized (tracker) {
                        tracker.registerDuration(PerformanceTracker.Type.DELETE, duration);
                    }
                }
            }
            catch (SQLException e) {
                log.error(e.getMessage(), e);
            }
        };
    }
}
