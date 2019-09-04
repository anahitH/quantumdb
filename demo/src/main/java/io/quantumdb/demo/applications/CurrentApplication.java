package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Random;

import io.quantumdb.demo.utils.PerformanceTracker;
import io.quantumdb.demo.utils.UserUtils;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class CurrentApplication extends Application {
    private final Random random;

    public CurrentApplication(String url, String server, String database, String user, String pass) {
        super(url, server, database, user, pass);
        this.random = new Random();
    }

    PerformanceTracker.ExecutionStats performSelect(Connection conn) throws SQLException {
        //System.out.println("PerformSelect");
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            String query = "SELECT id, name, email FROM users WHERE id = " + id;
            Date start = new Date();
            try (ResultSet resultSet = statement.executeQuery(query)) {
                // Do nothing with the resultSet.
                Date end = new Date();
                return new PerformanceTracker.ExecutionStats(start, end, 0);
            }
        }
    }

    PerformanceTracker.ExecutionStats performUpdate(Connection conn) throws SQLException {
        //System.out.println("PerformUpdate");
        String newName = UserUtils.pickName(random);
        String newEmail = UserUtils.getEmail(newName);
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            String query = "UPDATE users SET name = '" + newName + "', email = '" + newEmail + "' WHERE id = " + id;
            Date start = new Date();
            int updatedNum = statement.executeUpdate(query);
            Date end = new Date();
            return new PerformanceTracker.ExecutionStats(start, end, updatedNum);
        }
    }

    PerformanceTracker.ExecutionStats performInsert(Connection conn) throws SQLException {
        //System.out.println("PerformInsert");
        String newName = UserUtils.pickName(random);
        String newEmail = UserUtils.getEmail(newName);

        try (Statement statement = conn.createStatement()) {
            String query = "INSERT INTO users (name, email) VALUES ('" + newName + "', '" + newEmail + "')";
            Date start = new Date();
            int rowsUpdated = statement.executeUpdate(query);
            Date end = new Date();
            return new PerformanceTracker.ExecutionStats(start, end, rowsUpdated);
        }
    }

    PerformanceTracker.ExecutionStats performDelete(Connection conn) throws SQLException {
        //System.out.println("PerformDelete");
        int id = random.nextInt(TABLE_SIZE);

        try (Statement statement = conn.createStatement()) {
            String query = "DELETE FROM users WHERE id = " + id;
            Date start = new Date();
            int rowsUpdated = statement.executeUpdate(query);
            Date end = new Date();

            return new PerformanceTracker.ExecutionStats(start, end, rowsUpdated);
        }
    }

}
