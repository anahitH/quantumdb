package io.quantumdb.demo;

import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static java.lang.Thread.sleep;

import java.sql.Connection;
import java.sql.SQLException;

import io.quantumdb.core.backends.Backend;
import io.quantumdb.core.backends.Config;
import io.quantumdb.core.backends.DatabaseMigrator;
import io.quantumdb.core.migration.Migrator;
import io.quantumdb.core.planner.PostgresqlBackend;
//import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.schema.definitions.Catalog;
//import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.State;
import io.quantumdb.core.versioning.Version;
import io.quantumdb.demo.applications.CurrentApplication;
import io.quantumdb.demo.applications.NewApplication;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;



@Data
@Slf4j
@EqualsAndHashCode(callSuper = true)
public class MigrationWithQuantumDB extends Migration {

    private final Migrator migrator;
    private final Backend backend;
    private final String catalog;

    public MigrationWithQuantumDB(String url, String catalog, String user, String pass) {
        // modify server and database when actually using this class
        super(url, "", user, pass, "");
        this.catalog = catalog;
        Config config = new Config();
        config.setUrl(url);
        config.setCatalog(catalog);
        config.setUser(user);
        config.setPassword(pass);
        config.setDriver("io.quantumdb.driver.Driver");
        this.backend = new PostgresqlBackend(config);
        this.migrator = new Migrator(this.backend);
    }

    public void run() throws InterruptedException, ClassNotFoundException, SQLException {
        Connection connection = createConnection();

        log.info("Setting up database for demo...");
        createUserTable(connection);
        fillUserTable();

        Changelog changelog = prepareSchemaChange();
        String quantumCurrentUrl = createUrl(changelog.getRoot());

        //sleep(5_000);
        //log.info("Starting the application (version 1)...");
        //CurrentApplication current = new CurrentApplication(quantumCurrentUrl, getUser(), getPass());
        //current.run();

        //sleep(10_000);
        //log.info("Application (version 1) performance before schema change: \n\n" + current.getPerformance());

        sleep(2_000);
        log.info("Performing a schema change...");
        try {
            performSchemaChange(changelog);
        } catch (DatabaseMigrator.MigrationException e) {
            return;
        }

        //sleep(10_000);
        //log.info("Starting the application (version 2)...");
        //String quantumNextUrl = createUrl(changelog.getLastAdded());
        //NewApplication next = new NewApplication(quantumNextUrl, getUser(), getPass());
        //next.run();

        //sleep(5_000);
        //log.info("Application (version 1) performance during schema change: \n\n" + current.getPerformance());

        //sleep(10_000);
        //log.info("Stop the application (version 1)...");
        //current.stop();

        //sleep(5_000);
        //log.info("Application (version 2) performance while application (version 1) was running: \n\n" + next.getPerformance());

        //sleep(10_000);
        //log.info("Stop the application (version 2)...");
        //next.stop();

        //sleep(2_000);
        //log.info("Tearing down the database...");
		//tearDown(connection);

        // Print performance info
        //log.info("Application (version 2) performance after application (version 1) was terminated: \n\n" + next.getPerformance());
    }

    private String createUrl(Version version) {
        String url = getUrl();
        url = url.replace("jdbc:postgresql://", "jdbc:quantumdb:postgresql://");
        url += "?version=" + version.getId();
        return url;
    }

    private Changelog prepareSchemaChange() throws SQLException {
        State state = backend.loadState();
        Changelog changelog = state.getChangelog();

        // Register pre-existing tables in root version.
        //TableMapping mapping = state.getTableMapping();
        Catalog catalog = state.getCatalog();
        for (Table table : catalog.getTables()) {
            state.getRefLog().addTable(table.getName(), table.getName(), changelog.getRoot());
            //mapping.set(changelog.getRoot(), table.getName(), table.getName());
        }

        // Add schema change.
        changelog.addChangeSet("Michael de Jong", "Anahit",
                addColumn("users", "activated_account", bool(), "false", NOT_NULL));

        backend.persistState(state);

        return changelog;
    }

    @Override
    protected void tearDown(Connection connection) throws SQLException {
        super.tearDown(connection);
        execute(connection, "DROP TABLE IF EXISTS quantumdb_tablemappings");
        execute(connection, "DROP TABLE IF EXISTS quantumdb_changesets");
        execute(connection, "DROP TABLE IF EXISTS quantumdb_changelog");
    }

    @Override
    protected void performSchemaChange(Connection connection) throws SQLException {
        // Do nothing. We're using performSchemaChange(Changelog) instead.
    }

    protected void performSchemaChange(Changelog changelog) throws DatabaseMigrator.MigrationException {
        long start = System.currentTimeMillis();

        String sourceVersionId = changelog.getRoot().getId();
        String targetVersionId = changelog.getLastAdded().getId();
        try {
            migrator.migrate(sourceVersionId, targetVersionId);
        } catch (DatabaseMigrator.MigrationException e) {
            log.info("  Schema migration failed with exception {}", e.toString());
            throw e;
        }

        long end = System.currentTimeMillis();
        log.info("  Schema was modified, took: {}ms", (end - start));
    }

}
