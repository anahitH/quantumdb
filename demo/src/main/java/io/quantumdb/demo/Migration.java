package io.quantumdb.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.quantumdb.demo.utils.UserUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class Migration {

    private final String url;
    private final String user;
    private final String pass;

    public abstract void run() throws InterruptedException, ClassNotFoundException, SQLException;

    protected Connection createConnection() throws ClassNotFoundException, SQLException {
        return DriverManager.getConnection(url, user, pass);
    }

    protected void createUserTable(Connection connection) throws SQLException {
        execute(connection, "CREATE SEQUENCE users_id_seq");

        execute(connection, new StringBuilder()
                .append("CREATE TABLE users (")
                .append("  id bigint NOT NULL DEFAULT NEXTVAL('users_id_seq'), ")
                .append("  name varchar(64) NOT NULL, ")
                .append("  email varchar(255) NOT NULL, ")
                .append("  PRIMARY KEY (id)")
                .append(");")
                .toString());

        execute(connection, "ALTER SEQUENCE users_id_seq OWNED BY users.id");
    }

    protected void fillUserTable() throws InterruptedException {
        ExecutorService executorService = new ScheduledThreadPoolExecutor(4);
        Random random = new Random();

        AtomicInteger progressCounter = new AtomicInteger();
        for (int i = 0; i < 500; i++) {
            executorService.submit(() -> {
                try (Connection connection = createConnection()) {
                    String query = "INSERT INTO users (name, email) VALUES (?, ?)";
                    try (PreparedStatement statement = connection.prepareStatement(query)) {
                        for (int j = 0; j < 100_000; j++) {
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

    protected abstract void performSchemaChange(Connection connection) throws SQLException;

    protected void tearDown(Connection connection) throws SQLException {
        execute(connection, "DROP TABLE IF EXISTS users");
    }

    protected void execute(Connection connection, String query) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(query);
        }
    }

}
