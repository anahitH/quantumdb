package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import io.quantumdb.demo.utils.UserUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class CurrentApplication extends Application {
    private final Random random;

    public CurrentApplication(String url, String user, String pass) {
        super(url, user, pass);
        this.random = new Random();
    }

    int performSelect(Connection conn) throws SQLException {
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            long start = System.currentTimeMillis();
            long end;
            try (ResultSet resultSet = statement.executeQuery("SELECT id, name, email FROM users WHERE id = " + id)) {
                // Do nothing with the resultSet.
                end = System.currentTimeMillis();
            }

            return (int) (end - start);
        }
    }

    int performUpdate(Connection conn) throws SQLException {
        String newName = UserUtils.pickName(random);
        String newEmail = UserUtils.getEmail(newName);
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            long start = System.currentTimeMillis();
            String query = "UPDATE users SET name = '" + newName + "', email = '" + newEmail + "' WHERE id = " + id;
            statement.executeUpdate(query);
            long end = System.currentTimeMillis();

            return (int) (end - start);
        }
    }

    int performInsert(Connection conn) throws SQLException {
        String newName = UserUtils.pickName(random);
        String newEmail = UserUtils.getEmail(newName);

        try (Statement statement = conn.createStatement()) {
            long start = System.currentTimeMillis();
            statement.executeUpdate("INSERT INTO users (name, email) VALUES ('" + newName + "', '" + newEmail + "')");
            long end = System.currentTimeMillis();

            return (int) (end - start);
        }
    }

    int performDelete(Connection conn) throws SQLException {
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            long start = System.currentTimeMillis();
            statement.executeUpdate("DELETE FROM users WHERE id = " + id);
            long end = System.currentTimeMillis();

            return (int) (end - start);
        }
    }
}
