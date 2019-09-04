package io.quantumdb.demo.applications;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import io.quantumdb.demo.utils.PerformanceTracker;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class SchemaChangeApplication {

    private static String RENAMED_COLUMN_NAME = "new_is_activated";
    private static String INDEX_NAME = "I_users1";
    private static String CONSTRAINT_NAME = "";
    private static String CLUSTERED_CONSTRAINT_NAME = "";
    private static String COLUMN_NAME_WITH_NULL_DEFAULT_VALUE = "is_activated";
    private static String COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE = "non_null_string";
    private static String COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE = "nvarchar_max_string_default_null";
    private static String COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE = "nvarchar_max_string_default_non_null";

    public enum DDL_TYPE {
        ADD_COLUMN(0),
        DROP_COLUMN(1),
        ADD_COLUMN_WITH_CONSTRAINT(2),
        RENAME_COLUMN(3),
        MODIFY_COLUMN_INCREASE_STRING_DATATYPE(4),
        MODIFY_COLUMN_SHRINK_STRING_DATATYPE(5),

        CREATE_IDX(6),
        CREATE_IDX_ONLINE(7),
        REBUILD_IDX(8),
        REBUILD_IDX_ONLINE(9),
        DROP_IDX(10),

        ADD_CLUSTERED_INDEX(11),
        DROP_CLUSTERED_INDEX(12),
        ADD_CLUSTERED_INDEX_ONLINE(13),
        DROP_CLUSTERED_INDEX_ONLINE(14),

        ADD_CONSTRAINT(15),
        DROP_CONSTRAINT(16),

        // TODO: need to execute ones bellow
        COPY_TABLE(17),
        COPY_COLUMN_TO_THE_SAME_DATATYPE(18),
        COPY_COLUMN_TO_SMALLER_DATATYPE(19),
        COPY_COLUMN_TO_LARGER_DATATYPE(20),
        RENAME_TABLE(21),

        ADD_COLUMN_DEFAULT_VALUE_NOT_NULL(22),
        ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL(23),
        ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL(24),

        DROP_COLUMN_DEFAULT_VALUE_NOT_NULL(25),
        DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL(26),
        DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL(27),

        CREATE_FULLTEXT_INDEX(28),

        NUM_DDL(29),
        MODIFY_CONSTRAINT_COLUMN (1000);

        private final int value;
        private static List<DDL_TYPE> enumToInt = new ArrayList<>(NUM_DDL.getValue());

        DDL_TYPE(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public static DDL_TYPE getDDLOp(int value)
        {
            if (enumToInt.isEmpty()) {
                enumToInt.add(ADD_COLUMN.getValue(), ADD_COLUMN);
                enumToInt.add(DROP_COLUMN.getValue(), DROP_COLUMN);
                enumToInt.add(ADD_COLUMN_WITH_CONSTRAINT.getValue(), ADD_COLUMN_WITH_CONSTRAINT);
                enumToInt.add(RENAME_COLUMN.getValue(), RENAME_COLUMN);
                enumToInt.add(MODIFY_COLUMN_INCREASE_STRING_DATATYPE.getValue(), MODIFY_COLUMN_INCREASE_STRING_DATATYPE);
                enumToInt.add(MODIFY_COLUMN_SHRINK_STRING_DATATYPE.getValue(), MODIFY_COLUMN_SHRINK_STRING_DATATYPE);

                //enumToInt.add(MODIFY_CONSTRAINT_COLUMN.getValue(), MODIFY_CONSTRAINT_COLUMN);
                enumToInt.add(CREATE_IDX.getValue(), CREATE_IDX);
                enumToInt.add(CREATE_IDX_ONLINE.getValue(), CREATE_IDX);
                enumToInt.add(REBUILD_IDX.getValue(), REBUILD_IDX);
                enumToInt.add(REBUILD_IDX_ONLINE.getValue(), REBUILD_IDX_ONLINE);
                enumToInt.add(DROP_IDX.getValue(), DROP_IDX);
                enumToInt.add(ADD_CLUSTERED_INDEX.getValue(), ADD_CLUSTERED_INDEX);
                enumToInt.add(DROP_CLUSTERED_INDEX.getValue(), DROP_CLUSTERED_INDEX);
                enumToInt.add(ADD_CLUSTERED_INDEX_ONLINE.getValue(), ADD_CLUSTERED_INDEX_ONLINE);
                enumToInt.add(DROP_CLUSTERED_INDEX_ONLINE.getValue(), DROP_CLUSTERED_INDEX_ONLINE);

                enumToInt.add(ADD_CONSTRAINT.getValue(), ADD_CONSTRAINT);
                enumToInt.add(DROP_CONSTRAINT.getValue(), DROP_CONSTRAINT);

                enumToInt.add(COPY_TABLE.getValue(), COPY_TABLE);
                enumToInt.add(COPY_COLUMN_TO_THE_SAME_DATATYPE.getValue(), COPY_COLUMN_TO_THE_SAME_DATATYPE);
                enumToInt.add(COPY_COLUMN_TO_SMALLER_DATATYPE.getValue(), COPY_COLUMN_TO_SMALLER_DATATYPE);
                enumToInt.add(COPY_COLUMN_TO_LARGER_DATATYPE.getValue(), COPY_COLUMN_TO_LARGER_DATATYPE);
                enumToInt.add(RENAME_TABLE.getValue(), RENAME_TABLE);

                enumToInt.add(ADD_COLUMN_DEFAULT_VALUE_NOT_NULL.getValue(), ADD_COLUMN_DEFAULT_VALUE_NOT_NULL);
                enumToInt.add(ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL.getValue(), ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL);
                enumToInt.add(ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL.getValue(), ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL);

                // Don't execute those
                enumToInt.add(DROP_COLUMN_DEFAULT_VALUE_NOT_NULL.getValue(), DROP_COLUMN_DEFAULT_VALUE_NOT_NULL);
                enumToInt.add(DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL.getValue(), DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL);
                enumToInt.add(DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL.getValue(), DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL);

                enumToInt.add(CREATE_FULLTEXT_INDEX.getValue(), CREATE_FULLTEXT_INDEX);

                enumToInt.add(NUM_DDL.getValue(), NUM_DDL);
            }
            if (value > NUM_DDL.getValue()) {
                throw new RuntimeException("Invalid value for enum");
            }
            return enumToInt.get(value);
        }

        public static String getDDLName(DDL_TYPE ddlOp)
        {
            switch (ddlOp)
            {
                case ADD_COLUMN:
                    return "AddColumn";
                case DROP_COLUMN:
                    return "DropColumn";
                case ADD_COLUMN_WITH_CONSTRAINT:
                    return "AddColumnWithConstraint";
                case RENAME_COLUMN:
                    return "RenameColumn";
                case MODIFY_COLUMN_INCREASE_STRING_DATATYPE:
                    return "ModifyColumnIncreaseStringDatatype";
                case MODIFY_COLUMN_SHRINK_STRING_DATATYPE:
                    return "ModifyColumnShrinkStringDatatype";
                case MODIFY_CONSTRAINT_COLUMN:
                    return "ModifyConstraintColumn";
                case CREATE_IDX:
                    return "CreateIndex";
                case CREATE_IDX_ONLINE:
                    return "CreateIndexOnline";
                case REBUILD_IDX:
                    return "RebuildIndex";
                case REBUILD_IDX_ONLINE:
                    return "RebuildIndexOffline";
                case DROP_IDX:
                    return "DropIndex";
                case ADD_CLUSTERED_INDEX:
                    return "AddClusteredIndex";
                case ADD_CLUSTERED_INDEX_ONLINE:
                    return "AddClusteredIndexOnline";
                case DROP_CLUSTERED_INDEX:
                    return "DropClusteredIndex";
                case DROP_CLUSTERED_INDEX_ONLINE:
                    return "DropClusteredIndexOnline";
                case ADD_CONSTRAINT:
                    return "AddConstraint";
                case DROP_CONSTRAINT:
                    return "DropConstraint";
                case COPY_TABLE:
                    return "CopyTable";
                case COPY_COLUMN_TO_THE_SAME_DATATYPE:
                    return "CopyColumnToTheSameDatatype";
                case COPY_COLUMN_TO_SMALLER_DATATYPE:
                    return "CopyColumnToSmallerDatatype";
                case COPY_COLUMN_TO_LARGER_DATATYPE:
                    return "CopyColumnToLargerDatatype";
                case RENAME_TABLE:
                    return "RenameTable";
                case ADD_COLUMN_DEFAULT_VALUE_NOT_NULL:
                    return "AddColumnWithNonNullDefaultValue";
                case DROP_COLUMN_DEFAULT_VALUE_NOT_NULL:
                    return "DropColumnWithNonNullDefaultValue";
                case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
                    return "AddColumnWithMaxNvarcharDatatypeAndDefaultNullValue";
                case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
                    return "DropColumnWithMaxNvarcharDatatypeAndDefaultNullValue";
                case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
                    return "AddColumnWithMaxNvarcharDatatypeAndDefaultNotNullValue";
                case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
                    return "DropColumnWithMaxNvarcharDatatypeAndNonNullDefaultValue";
                case CREATE_FULLTEXT_INDEX:
                    return "CreateFullTextIndex";
                case NUM_DDL:
                default:
                    break;
            }
            throw new RuntimeException("Invalid DDL_TYPE object");
        }
    }

    private List<Callable> ddlOperations = new ArrayList<>();
    private final String url;
    private final String server;
    private final String database;
    private final String username;
    private final String pass;

    public SchemaChangeApplication(String url, String server, String database, String username, String pass) {
        this.url = url;
        this.server = server;
        this.database = database;
        this.username = username;
        this.pass = pass;
        fillDDLOperations();
    }

    public PerformanceTracker.ExecutionStats runChange(DDL_TYPE ddlOp)
    {
        if (ddlOp.getValue() >= DDL_TYPE.NUM_DDL.getValue()) {
            throw new RuntimeException("Invalid DDL Operation");
        }
        try {
            return (PerformanceTracker.ExecutionStats) this.ddlOperations.get(ddlOp.getValue()).call();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public void prepareForDDL(DDL_TYPE ddlOp)
    {
        switch (ddlOp)  {
        case ADD_COLUMN:
            execute(createDropColumnIfExistsStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE));
            break;
        case DROP_COLUMN:
            execute(createColumnIfNotExistsStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE, "Bit", null));
            break;
        case ADD_COLUMN_WITH_CONSTRAINT:
            execute(createDropColumnIfExistsStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE));
            break;
        case MODIFY_CONSTRAINT_COLUMN:
            execute(renameColumnStatement(RENAMED_COLUMN_NAME, COLUMN_NAME_WITH_NULL_DEFAULT_VALUE));
            break;
        case CREATE_IDX:
        case CREATE_IDX_ONLINE:
            execute(dropIndexIfExists());
            break;
        case ADD_CLUSTERED_INDEX:
        case ADD_CLUSTERED_INDEX_ONLINE:
            execute(dropClusteredIndexIfExists());
            break;
        case REBUILD_IDX:
        case REBUILD_IDX_ONLINE:
        case DROP_IDX:
            execute(createIndexIfNotExistsStatement(false));
            break;
        case DROP_CLUSTERED_INDEX:
        case DROP_CLUSTERED_INDEX_ONLINE:
            execute(createIndexIfNotExistsStatement(true));
            break;
        case ADD_CONSTRAINT:
            execute(dropUniqueConstraintIfExistsStatement());
            break;
        case DROP_CONSTRAINT:
            execute(createUniqueConstraintIfNotExistsStatement());
            break;
        case COPY_COLUMN_TO_THE_SAME_DATATYPE:
            execute("Alter table users add name_copy nvarchar(64)");
            break;
        case COPY_COLUMN_TO_LARGER_DATATYPE:
            execute("Alter table users add email_copy nvarchar(255)");
            break;
        case ADD_COLUMN_DEFAULT_VALUE_NOT_NULL:
            execute(createDropColumnIfExistsStatement(COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE));
            break;
        case DROP_COLUMN_DEFAULT_VALUE_NOT_NULL:
            //execute(createColumnIfNotExistsStatement(COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE, "nvarchar(8)", ""));
            break;
        case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
            execute(createDropColumnIfExistsStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE));
            break;
        case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
            //execute(createColumnIfNotExistsStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE, "nvarchar(max)", null));
            break;
        case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
            execute(createDropColumnIfExistsStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE));
            break;
        case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
            //execute(createColumnIfNotExistsStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE, "nvarchar(max)", "1"));
            break;
        case RENAME_COLUMN:
        case MODIFY_COLUMN_INCREASE_STRING_DATATYPE:
        case MODIFY_COLUMN_SHRINK_STRING_DATATYPE:
        case COPY_TABLE:
        case COPY_COLUMN_TO_SMALLER_DATATYPE:
        case RENAME_TABLE: // if this is the last operation, then there is no need to dp anything here. otherwise the following operations need to rename it back
        case CREATE_FULLTEXT_INDEX:
        case NUM_DDL:
        default:
            break;
        }
    }



    private void fillDDLOperations()
    {
        // TODO: add logging of runtime for each ddl
        ddlOperations.add(DDL_TYPE.ADD_COLUMN.getValue(), () -> execute(createColumnStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE, "Bit", null)));
        ddlOperations.add(DDL_TYPE.DROP_COLUMN.getValue(), () -> execute(dropColumnStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE)));
        ddlOperations.add(DDL_TYPE.ADD_COLUMN_WITH_CONSTRAINT.getValue(), () -> execute(createColumnStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE, "Bit", "0")));
        ddlOperations.add(DDL_TYPE.RENAME_COLUMN.getValue(), () -> execute(renameColumnStatement(COLUMN_NAME_WITH_NULL_DEFAULT_VALUE, RENAMED_COLUMN_NAME)));
        ddlOperations.add(DDL_TYPE.MODIFY_COLUMN_INCREASE_STRING_DATATYPE.getValue(), () -> execute("ALTER TABLE users ALTER COLUMN name nvarchar(255) WITH (ONLINE = ON)"));
        ddlOperations.add(DDL_TYPE.MODIFY_COLUMN_SHRINK_STRING_DATATYPE.getValue(), () -> execute("ALTER TABLE users ALTER COLUMN name nvarchar(64) WITH (ONLINE = ON)"));

        ddlOperations.add(DDL_TYPE.CREATE_IDX.getValue(), () -> execute(createIndexStatement(false, false)));
        ddlOperations.add(DDL_TYPE.CREATE_IDX_ONLINE.getValue(), () -> execute(createIndexStatement(true, false)));
        ddlOperations.add(DDL_TYPE.REBUILD_IDX.getValue(), () -> execute(rebuildIndex(false)));
        ddlOperations.add(DDL_TYPE.REBUILD_IDX_ONLINE.getValue(), () -> execute(rebuildIndex((true))));
        ddlOperations.add(DDL_TYPE.DROP_IDX.getValue(), () -> execute(dropIndex(false)));

        ddlOperations.add(DDL_TYPE.ADD_CLUSTERED_INDEX.getValue(), () -> execute(createIndexStatement(false, true)));
        ddlOperations.add(DDL_TYPE.DROP_CLUSTERED_INDEX.getValue(), () -> execute(dropClusteredIndex(false)));
        ddlOperations.add(DDL_TYPE.ADD_CLUSTERED_INDEX_ONLINE.getValue(), () -> execute(createIndexStatement(true, true)));
        ddlOperations.add(DDL_TYPE.DROP_CLUSTERED_INDEX_ONLINE.getValue(), () -> execute(dropClusteredIndex(true)));

        ddlOperations.add(DDL_TYPE.ADD_CONSTRAINT.getValue(), () -> execute(createUniqueConstraintStatement()));
        ddlOperations.add(DDL_TYPE.DROP_CONSTRAINT.getValue(), () -> execute(dropUniqueConstraintStatement()));

        ddlOperations.add(DDL_TYPE.COPY_TABLE.getValue(), () -> execute("SELECT *  INTO users_copy FROM  users"));
        ddlOperations.add(DDL_TYPE.COPY_COLUMN_TO_THE_SAME_DATATYPE.getValue(), () -> execute("update users set name_copy = name"));
        ddlOperations.add(DDL_TYPE.COPY_COLUMN_TO_SMALLER_DATATYPE.getValue(), () -> execute("update users set name_copy = email"));
        ddlOperations.add(DDL_TYPE.COPY_COLUMN_TO_LARGER_DATATYPE.getValue(), () -> execute("update users set email_copy = name"));
        ddlOperations.add(DDL_TYPE.RENAME_TABLE.getValue(), () -> execute("EXEC sp_rename 'users', 'users_renamed';"));

        ddlOperations.add(DDL_TYPE.ADD_COLUMN_DEFAULT_VALUE_NOT_NULL.getValue(), () -> execute(createColumnStatement(COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE, "nvarchar(8)", "bla")));
        ddlOperations.add(DDL_TYPE.ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL.getValue(), () -> execute(createColumnStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE, "nvarchar(max)", null)));
        ddlOperations.add(DDL_TYPE.ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL.getValue(), () -> execute(createColumnStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE, "nvarchar(max)", "blablabla")));

        ddlOperations.add(DDL_TYPE.DROP_COLUMN_DEFAULT_VALUE_NOT_NULL.getValue(), () -> execute(dropColumnStatement(COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE)));
        ddlOperations.add(DDL_TYPE.DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL.getValue(), () -> execute(dropColumnStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE)));
        ddlOperations.add(DDL_TYPE.DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL.getValue(), () -> execute(dropColumnStatement(COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE)));

        ddlOperations.add(DDL_TYPE.CREATE_FULLTEXT_INDEX.getValue(), () -> execute(createFullTextIndexStatement()));
    }


    private PerformanceTracker.ExecutionStats execute(String query)
    {
        System.out.println(query);
        PerformanceTracker.ExecutionStats executionStats = null;
        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            Date start = new Date();
            statement.execute(query);
            Date end = new Date();
            executionStats = new PerformanceTracker.ExecutionStats(start, end, 0);
        }
        catch (SQLException e) {
            System.out.println("Failed to execute statement\n" + query);
            e.printStackTrace();
        }
        return executionStats;
    }

    private Connection getConnection()
    {
        Properties props = new Properties();
        props.setProperty("serverName", server);
        props.setProperty("user", username);
        props.setProperty("password", pass);
        props.setProperty("databaseName", database);
        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e)
        {
            System.out.println("Failed to create connection");
        }
        return null;
    }

    private String createUniqueConstraintIfNotExistsStatement()  {
        StringBuilder createConstraintStr = new StringBuilder();
        createConstraintStr.append("IF NOT EXISTS (SELECT name from sys.objects where name like \'C_user\')");
        createConstraintStr.append(System.lineSeparator());
        createConstraintStr.append(createUniqueConstraintStatement());
        return createConstraintStr.toString();
    }

    private String createUniqueConstraintStatement()
    {
        StringBuilder createConstraintStr = new StringBuilder();
        createConstraintStr.append("ALTER TABLE users");
        createConstraintStr.append(System.lineSeparator());
        createConstraintStr.append("ADD CONSTRAINT C_user UNIQUE (id, name)");
        return createConstraintStr.toString();
    }

    private String dropUniqueConstraintIfExistsStatement()  {
        StringBuilder dropConstraintStr = new StringBuilder();
        dropConstraintStr.append("ALTER TABLE users");
        dropConstraintStr.append(System.lineSeparator());
        dropConstraintStr.append("DROP CONSTRAINT IF EXISTS C_user");
        dropConstraintStr.append(System.lineSeparator());
        return dropConstraintStr.toString();
    }

    private String dropUniqueConstraintStatement()
    {
        StringBuilder dropConstraintStr = new StringBuilder();
        dropConstraintStr.append("ALTER TABLE users");
        dropConstraintStr.append(System.lineSeparator());
        dropConstraintStr.append("DROP CONSTRAINT C_user");
        dropConstraintStr.append(System.lineSeparator());
        return dropConstraintStr.toString();
    }

    private java.lang.String createIndexIfNotExistsStatement(boolean isClustered) {
        StringBuilder createIndexStr = new StringBuilder();
        createIndexStr.append("IF NOT EXISTS (SELECT name from sys.indexes where name = '");
        createIndexStr.append(INDEX_NAME);
        createIndexStr.append("')");
        createIndexStr.append(createIndexStatement(false, isClustered));
        return createIndexStr.toString();
    }

    private String createIndexStatement(boolean isOnline, boolean isClustered)
    {
        StringBuilder createIndexStr = new StringBuilder();
        createIndexStr.append("CREATE ");
        if (isClustered) {
            createIndexStr.append("CLUSTERED");
        } else {
            createIndexStr.append("NONCLUSTERED");
        }
        createIndexStr.append(" INDEX [");
        createIndexStr.append(INDEX_NAME);
        createIndexStr.append("] ON users ([name] ASC, [email] ASC) ");
        if (isOnline) {
            createIndexStr.append("WITH (ONLINE=ON)");
        }
        createIndexStr.append(" ON [PRIMARY]");
        return createIndexStr.toString();
    }

    private String dropIndexIfExists() {
        return "DROP INDEX IF EXISTS [" + INDEX_NAME + "] on users";
    }

    private String dropClusteredIndexIfExists() {
        //declare @pki_name nvarchar(255)
        //set @pki_name = (select name from sys.key_constraints where name like '%users%')
        //print @pki_name
        //        EXECUTE ('ALTER TABLE users DROP CONSTRAINT IF EXISTS [' + @pki_name + ']')
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("declare @pki_name nvarchar(255)");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("set @pki_name = (select name from sys.indexes where name like '%users%')");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("IF @pki_name <> ''");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("EXECUTE ('DROP INDEX [' + @pki_name + '] on users')");
        return strBuilder.toString();
    }

    private String dropClusteredIndex(boolean isOnline)
    {
        StringBuilder strBuilder = new StringBuilder();
        //strBuilder.append("ALTER TABLE users DROP CONSTRAINT [");
        strBuilder.append("DROP INDEX [");
        strBuilder.append(INDEX_NAME);
        strBuilder.append("]");
        strBuilder.append(" on users ");
        if (isOnline) {
            strBuilder.append(" WITH (ONLINE = ON)");
        }
        return strBuilder.toString();
    }

    private String rebuildIndex(boolean isOnline)
    {
        String rebuildIndex = "ALTER INDEX " + INDEX_NAME + " on users REBUILD";
        if (isOnline) {
            rebuildIndex += " WITH (ONLINE = ON);";
        }
        return rebuildIndex;
    }

    private String dropIndex(boolean isOnline)
    {
        String dropIndexStr = "DROP INDEX [" + INDEX_NAME + "] on users";
        if (isOnline) {
            dropIndexStr += " WITH (ONLINE = ON)";
        }
        return dropIndexStr;
    }

    private String renameColumnStatement(String oldName, String newName)
    {
        return "exec sp_rename 'users." + oldName + "', '" + newName + "', 'COLUMN';";

    }

    private String dropColumnStatement(String columnName) {
        return "ALTER TABLE users DROP COLUMN " + columnName;
    }

    private String createDropColumnIfExistsStatement(String columnName) {
        return "ALTER TABLE users DROP COLUMN IF EXISTS " + columnName;

    }

    private String createColumnStatement(String columnName, String datatype, String defaultValue) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("ALTER TABLE users ADD ");
        strBuilder.append(columnName);
        strBuilder.append(" ");
        strBuilder.append(datatype);
        if (defaultValue != null) {
            strBuilder.append(" ");
            strBuilder.append("NOT NULL DEFAULT('");
            strBuilder.append(defaultValue);
            strBuilder.append("')");
        }
        return strBuilder.toString();
    }


    private String createColumnIfNotExistsStatement(String columnName, String datatype, String defaultValue) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("IF NOT EXISTS (");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'users'");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("AND column_name = '");
        strBuilder.append(columnName);
        strBuilder.append("')");
        strBuilder.append(System.lineSeparator());
        strBuilder.append(createColumnStatement(columnName, datatype, defaultValue));
        return strBuilder.toString();
    }

    private String createFullTextIndexStatement() {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("CREATE FULLTEXT INDEX ON users(email)");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("KEY INDEX UQ_id");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("WITH STOPLIST = SYSTEM");
        return strBuilder.toString();
    }

}
