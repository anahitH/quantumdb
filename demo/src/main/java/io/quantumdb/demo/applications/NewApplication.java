package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import io.quantumdb.demo.utils.PerformanceTracker;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@EqualsAndHashCode(callSuper = true)
@Data
@Slf4j
public class NewApplication extends Application {

    private final Random random;

    public NewApplication(String url, String user, String pass) {
        super(url, "", "", user, pass);
        this.random = new Random();
    }

    PerformanceTracker.ExecutionStats performSelect(Connection conn) throws SQLException {
        System.out.println("Perform select");
        return null;
        //int id = random.nextInt(TABLE_SIZE);

        //try (Statement statement = conn.createStatement()) {
        //    long start = System.currentTimeMillis();
        //    long end;
        //    try (ResultSet resultSet = statement.executeQuery("SELECT id, name, email, activated_account FROM users WHERE id = " + id)) {
        //        // Do nothing with the resultSet.
        //        end = System.currentTimeMillis();
        //    }

        //    return (int) (end - start);
        //}
    }

    PerformanceTracker.ExecutionStats performUpdate(Connection conn) throws SQLException {
        System.out.println("Perform update");
        return null;
        //int id = random.nextInt(TABLE_SIZE);
        //String newName = UserUtils.pickName(random);
        //String newEmail = UserUtils.getEmail(newName);
        //boolean activated = random.nextBoolean();

        //String query = "UPDATE users SET name = '" + newName + "', email = '" + newEmail + "', activated_account = "
        //        + activated + " WHERE id = " + id;

        //long start = System.currentTimeMillis();
        //try (Statement statement = conn.createStatement()) {
        //    statement.executeUpdate(query);
        //    long end = System.currentTimeMillis();

        //    return (int) (end - start);
        //}
    }

    PerformanceTracker.ExecutionStats performInsert(Connection conn) throws SQLException {
        System.out.println("Perform insert");
        return null;
        //String newName = UserUtils.pickName(random);
        //String newEmail = UserUtils.getEmail(newName);
        //boolean activated = random.nextBoolean();

        //String query = "INSERT INTO users (name, email, activated_account) VALUES ('" + newName + "', '"
        //        + newEmail + "', " + activated + ")";

        //long start = System.currentTimeMillis();
        //try (Statement statement = conn.createStatement()) {
        //    statement.executeUpdate(query);
        //    long end = System.currentTimeMillis();

        //    return (int) (end - start);
        //}
    }

    PerformanceTracker.ExecutionStats performDelete(Connection conn) throws SQLException {
        System.out.println("Perform delete");
        return null;
        //int id = random.nextInt(TABLE_SIZE);

        //long start = System.currentTimeMillis();
        //try (Statement statement = conn.createStatement()) {
        //    statement.executeUpdate("DELETE FROM users WHERE id = " + id);
        //    long end = System.currentTimeMillis();

        //    return (int) (end - start);
        //}
    }

}
