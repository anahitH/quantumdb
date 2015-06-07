package io.quantumdb.core.backends;

import java.sql.Connection;
import java.sql.SQLException;

import io.quantumdb.core.backends.postgresql.migrator.TableCreator;
import io.quantumdb.core.versioning.State;

public interface Backend {

	/**
	 * Loads the current state of the database schema and its evolution from the database.
	 *
	 * @return The current state of the database schema.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	State loadState() throws SQLException;

	/**
	 * Persists the current state of the database schema and its evolution to the database.
	 *
	 * @param state The current state of the database schema.
	 *
	 * @throws SQLException In case the database could not be reached, or queried correctly.
	 */
	void persistState(State state) throws SQLException;

	TableCreator getTableCreator();

	/**
	 * Creates a connection to the database.
	 *
	 * @return A database Connection object.
	 *
	 * @throws SQLException In case no connection to the database could be established.
	 */
	Connection connect() throws SQLException;

	/**
	 * @return The DatabaseMigrator implementation for this particular database.
	 */
	DatabaseMigrator getMigrator();

}
