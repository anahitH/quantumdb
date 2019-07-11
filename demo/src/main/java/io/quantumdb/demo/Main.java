package io.quantumdb.demo;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import lombok.Data;

public class Main {

    @Data
    private static class Config {
        private final String url;
        private final String user;
        private final String pass;
        private final String catalog;
        private final boolean useQuantumDB;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {
        Class.forName("io.quantumdb.driver.Driver");
        Class.forName("org.postgresql.Driver");

        Config config = parseParameters(args);

        if (config.useQuantumDB) {
            new MigrationWithQuantumDB(config.url, config.catalog, config.user, config.pass).run();
        }
        else {
            new MigrationWithDowntime(config.url, config.user, config.pass).run();
        }
    }

    private static Config parseParameters(String[] args) throws SQLException {
//        boolean useQuantum = false;
//        if (args[0].equalsIgnoreCase("quantumdb")) {
//            useQuantum = true;
//        }
//        else if (args[0].equalsIgnoreCase("with-downtime")) {
//            useQuantum = false;
//        }
//        else {
//            throw new IllegalArgumentException("You must specify either 'quantumdb' or 'with-downtime'.");
//        }

        String url = args[0];
        String user = args[1];
        String pass = args[2];

        checkArgument(!isNullOrEmpty(url), "You must specify a 'url'");
        checkArgument(!isNullOrEmpty(user), "You must specify a 'user'");
        checkArgument(!isNullOrEmpty(user), "You must specify a 'password'");

        String catalog = getCatalog(url, user, pass);

        return new Config(url, user, pass, catalog, false);
    }


    private static String getCatalog(String url, String user, String pass) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url, user, pass)) {
            try (Statement statement = connection.createStatement()) {
                ResultSet resultSet = statement.executeQuery("SELECT current_database();");

                if (resultSet.next()) {
                    return resultSet.getString(1);
                }
                throw new IllegalStateException("The database URL you've specified does not specify a catalog.");
            }
        }
    }

}
