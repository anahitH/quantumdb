package io.quantumdb.demo;

import static java.lang.Thread.sleep;

import java.sql.Connection;
import java.sql.SQLException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.quantumdb.demo.applications.CurrentApplication;
import io.quantumdb.demo.applications.NewApplication;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class MigrationWithDowntime extends Migration {

    public static void main(String[] args) throws InterruptedException, SQLException, ClassNotFoundException {
        String url = System.getProperty("url");
        String user = System.getProperty("user");
        String pass = System.getProperty("pass");

        Preconditions.checkArgument(!Strings.isNullOrEmpty(url), "You must specify a VM argument '-Durl'.");
        Preconditions.checkArgument(!Strings.isNullOrEmpty(user), "You must specify a VM argument '-Duser'.");

        new MigrationWithDowntime(url, user, pass).run();
    }

    public MigrationWithDowntime(String url, String user, String pass) {
        super(url, user, pass);
    }

    public void run() throws InterruptedException, ClassNotFoundException, SQLException {
        Connection connection = createConnection();

        log.info("Setting up database for demo...");
        createUserTable(connection);
        fillUserTable();

        sleep(5_000);
        log.info("Starting the application (version 1)...");
        CurrentApplication current = new CurrentApplication(getUrl(), getUser(), getPass());
        current.run();

        sleep(10_000);
        log.info("Application (version 1) performance before schema change: \n\n" + current.getPerformance());

        sleep(2_000);
        log.info("Performing a schema change...");
        performSchemaChange(connection);

        sleep(10_000);
        log.info("Starting the application (version 2)...");
        NewApplication next = new NewApplication(getUrl(), getUser(), getPass());
        next.run();

        sleep(5_000);
        log.info("Application (version 1) performance during schema change: \n\n" + current.getPerformance());

        sleep(10_000);
        log.info("Stop the application (version 1)...");
        current.stop();

        sleep(5_000);
        log.info("Application (version 2) performance while application (version 1) was running: \n\n" + next.getPerformance());

        sleep(10_000);
        log.info("Stop the application (version 2)...");
        next.stop();

        sleep(2_000);
        log.info("Tearing down the database...");
//		tearDown(connection);

        // Print performance info
        log.info("Application (version 2) performance after application (version 1) was terminated: \n\n" + next.getPerformance());
    }

    @Override
    protected void performSchemaChange(Connection connection) throws SQLException {
        long start = System.currentTimeMillis();
//		execute(connection, "CREATE SEQUENCE books_id_seq;");
//		execute(connection, "CREATE TABLE books (id bigint DEFAULT VALUE nextval(books_id_seq), name varchar(255) NOT NULL, primary key (id));");
        execute(connection, "ALTER TABLE users ADD COLUMN activated_account boolean NOT NULL DEFAULT false");
//		execute(connection, "ALTER TABLE users ADD COLUMN favorite_book bigint");
//		execute(connection, "ALTER TABLE users ADD FOREIGN KEY favorite_book REFERENCES books(id);");
//		execute(connection, "ALTER SEQUENCE books_id_seq OWNER TO books.id;");

        long end = System.currentTimeMillis();
        log.info("  Schema was modified, took: {}ms", (end - start));
    }

}
