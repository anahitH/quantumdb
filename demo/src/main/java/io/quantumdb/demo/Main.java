package io.quantumdb.demo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import lombok.Data;

public class Main {

    @Data
    private static class Config {
        private final String url;
        private final String server;
        private final String user;
        private final String pass;
        private final String database;
        private final String catalog;
        private final boolean useQuantumDB;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        //Class.forName("io.quantumdb.driver.Driver");
        //Class.forName("org.postgresql.Driver");
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");

        Config config = parseParameters(args);

        if (config.useQuantumDB) {
            new MigrationWithQuantumDB(config.url, config.catalog, config.user, config.pass).run();
        }
        else {
            new MigrationWithDowntime(config.url, config.server, config.user, config.pass, config.database).run();
        }
    }

    private static Config parseParameters(String[] args) throws SQLException {
        boolean useQuantum = false;
        if (args[0].equalsIgnoreCase("quantumdb")) {
            useQuantum = true;
        }
        else if (args[0].equalsIgnoreCase("with-downtime")) {
            useQuantum = false;
        }
        else {
            throw new IllegalArgumentException("You must specify either 'quantumdb' or 'with-downtime'.");
        }

        String url = args[1];
        String server = args[2];
        String user = args[3];
        String pass = args[4];
        String database = args[5];

        checkArgument(!isNullOrEmpty(url), "You must specify a 'url'");
        checkArgument(!isNullOrEmpty(server), "You must specify a 'server'");
        checkArgument(!isNullOrEmpty(user), "You must specify a 'user'");
        checkArgument(!isNullOrEmpty(user), "You must specify a 'password'");
        checkArgument(!isNullOrEmpty(database), "You must specify a 'database'");

        String catalog = getCatalog(url, server, user, pass, database);

        return new Config(url, server, user, pass, database, catalog, false);
    }


    private static String getCatalog(String url, String server, String user, String pass, String database) throws SQLException {
        Properties props = new Properties();
        props.setProperty("serverName", server);
        props.setProperty("user", user);
        props.setProperty("password", pass);
        props.setProperty("databaseName", database);
        try (Connection connection = DriverManager.getConnection(url, props)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT DB_NAME()");

                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                throw new IllegalStateException("The database URL you've specified does not specify a catalog.");
            }
        }
    }

}
