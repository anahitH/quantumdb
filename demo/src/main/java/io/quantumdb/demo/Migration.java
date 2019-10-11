package io.quantumdb.demo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.quantumdb.demo.utils.PerformanceTracker;
import io.quantumdb.demo.utils.UserUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class Migration {

    private final String url;
    private final String server;
    private final String user;
    private final String pass;
    private final String database;
    private final String tableName;

    public abstract void run() throws InterruptedException, ClassNotFoundException, SQLException, IOException, URISyntaxException;

    protected Connection createConnection() throws ClassNotFoundException, SQLException {
        Properties props = new Properties();
        props.setProperty("serverName", server);
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("databaseName", database);
        return DriverManager.getConnection(url, props);
    }


    protected void createUserTableWithLOBColumn(Connection connection) throws SQLException {
        execute(connection, new StringBuilder()
                .append("CREATE TABLE users_full_text_idx  (")
                .append("  id bigint NOT NULL DEFAULT (NEXT VALUE FOR users_id_seq), ")
                .append("  name varchar(64) NOT NULL, ")
                .append("  email varchar(255) NOT NULL, ")
                .append("  LOB_data varchar(MAX) NOT NULL, ")
                .append("  PRIMARY KEY (id)")
                .append(");")
                .toString());
        execute(connection, "CREATE FULLTEXT CATALOG fulltextCatalog AS DEFAULT;");
    }

    protected void createUserTable(Connection connection) throws SQLException {
        execute(connection, "CREATE SEQUENCE users_id_seq START WITH 0 INCREMENT BY 1");

        execute(connection, new StringBuilder()
                .append("CREATE TABLE " + this.tableName + " (")
                .append("  id bigint NOT NULL DEFAULT (NEXT VALUE FOR users_id_seq), ")
                .append("  name varchar(64) NOT NULL, ")
                .append("  email varchar(255) NOT NULL, ")
                .append("  PRIMARY KEY (id)")
                .append(");")
                .toString());

        // Can not find equivalent of OWNED BY in sql server
        // execute(connection, "ALTER SEQUENCE users_id_seq OWNED BY users.id");
    }

    protected void createBackupUserTable(Connection connection) throws SQLException {
        execute(connection, "select * into users_backup from users");
    }

    protected void fillUsersFullTextIndexTable() throws InterruptedException, URISyntaxException, IOException {
        String content = Files.readString(Paths.get(".", "demo.log"));
        System.out.println(content.length());
        ExecutorService executorService = new ScheduledThreadPoolExecutor(10);
        Random random = new Random();

        AtomicInteger progressCounter = new AtomicInteger();
        for (int i = 0; i < 500000; i++) {
            executorService.submit(() -> {
                try (Connection connection = createConnection()) {
                    String query = "INSERT INTO users_full_text_idx (name, email, LOB_data) VALUES (?, ?, ?)";
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        for (int j = 0; j < 10; j++) {
                            String name = UserUtils.pickName(random);
                            String email = UserUtils.getEmail(name);

                            statement.setString(1, name);
                            statement.setString(2, email);
                            statement.setString(3, content);
                            statement.addBatch();
                        }
                        statement.executeBatch();
                    }
                    int progress = progressCounter.incrementAndGet();
                    if (progress % 50 == 0) {
                        log.info("  Filling table with data: {}%", progress / 5);
                    }
                } catch (ClassNotFoundException | SQLException e) {
                    log.error(e.getMessage(), e);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    protected void fillUserTable() throws InterruptedException {
        ExecutorService executorService = new ScheduledThreadPoolExecutor(4);
        Random random = new Random();

        AtomicInteger progressCounter = new AtomicInteger();
        for (int i = 0; i < 500; i++) {
            executorService.submit(() -> {
                try (Connection connection = createConnection()) {
                    String query = "INSERT INTO " + this.tableName + " (name, email) VALUES (?, ?)";
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        for (int j = 0; j < 60_000/*100_000*/; j++) {
                            String name = UserUtils.pickName(random);
                            String email = UserUtils.getEmail(name);

                            statement.setString(1, name);
                            statement.setString(2, email);
                            statement.addBatch();
                        }
                        statement.executeBatch();
                    }
                    int progress = progressCounter.incrementAndGet();
                    if (progress % 50 == 0) {
                        log.info("  Filling table with data: {}%", progress / 5);
                    }
                }
                catch (ClassNotFoundException | SQLException e) {
                    log.error(e.getMessage(), e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
    }

    protected abstract void performSchemaChange(Connection connection, PerformanceTracker.Type dmlType) throws SQLException, InterruptedException;

    protected void tearDown(Connection connection) throws SQLException {
        execute(connection, "DROP TABLE IF EXISTS " + this.tableName + " cascade");
    }

    protected void execute(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
    }

}
