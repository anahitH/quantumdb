package io.quantumdb.demo.applications;

import io.quantumdb.demo.utils.DDL_TYPE;
import io.quantumdb.demo.utils.ExecutionStats;
import io.quantumdb.demo.utils.TableConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class SchemaChange {

    protected final String url;
    protected final String server;
    protected final String database;
    protected final String username;
    protected final String pass;
    protected final String tableName;

    public SchemaChange(String url, String server, String database, String username, String pass, String tableName) {
        this.url = url;
        this.server = server;
        this.database = database;
        this.username = username;
        this.pass = pass;
        this.tableName = tableName;
    }

    public void prepareForDDL(DDL_TYPE ddlOp)
    {
        switch (ddlOp)  {
        case ADD_COLUMN:
            execute(createDropColumnIfExistsStatement(TableConstants.getUniqueColumnNameWithNullDefaultValue()));
            break;
        case ADD_COLUMN_WITH_CONSTRAINT:
            String columnName = TableConstants.getUniqueColumnNameWithNullDefaultValue();
            execute(createDropConstraintForColumnIfExistsStatement(columnName));
            execute(createDropColumnIfExistsStatement(columnName));
            break;
        case DROP_COLUMN:
            execute(createColumnIfNotExistsStatement(TableConstants.getUniqueColumnNameWithNullDefaultValue(), "Bit", null));
            break;
        case RENAME_COLUMN:
            columnName = TableConstants.getUniqueRenamedColumnName();
            execute(createDropColumnIfExistsStatement(columnName));
            TableConstants.resetUniqueNameIdGeneratorByStep(1);
            columnName = TableConstants.getUniqueColumnNameWithNullDefaultValue();
            execute(createColumnIfNotExistsStatement(columnName, "Bit", null));
            break;
        case MODIFY_COLUMN_INCREASE_STRING_DATATYPE:
            String new_column_name = TableConstants.getUniqueName("name");
            execute(createDropColumnIfExistsStatement(new_column_name));
            execute(createColumnIfNotExistsStatement(new_column_name, "nvarchar (64)", null));
            execute("update " + tableName + " set " + new_column_name + " = name");
            break;
        case MODIFY_COLUMN_SHRINK_STRING_DATATYPE:
            new_column_name = TableConstants.getUniqueName("email");
            execute(createDropColumnIfExistsStatement(new_column_name));
            execute(createColumnIfNotExistsStatement(new_column_name, "nvarchar (255)", null));
            execute("update " + tableName + " set " + new_column_name + " = email");
            break;
        case MODIFY_CONSTRAINT_COLUMN:
            execute(renameColumnStatement(TableConstants.getUniqueRenamedColumnName(), TableConstants.getUniqueColumnNameWithNullDefaultValue()));
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
            execute("Alter table " + tableName + " add name_copy nvarchar(64)");
            break;
        case COPY_COLUMN_TO_LARGER_DATATYPE:
            execute("Alter table " + tableName + " add email_copy nvarchar(255)");
            break;
        case ADD_COLUMN_DEFAULT_VALUE_NOT_NULL:
            execute(createDropColumnIfExistsStatement(TableConstants.getUniqueColumnNameWithNonNullDefaultValue()));
            break;
        case DROP_COLUMN_DEFAULT_VALUE_NOT_NULL:
            //execute(createColumnIfNotExistsStatement(TableConstants.getUniqueColumnNameWithNonNullDefaultValue(), "nvarchar(8)", ""));
            break;
        case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
            execute(createDropColumnIfExistsStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNullDefaultValue()));
            break;
        case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
            //execute(createColumnIfNotExistsStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNullDefaultValue(), "nvarchar(max)", null));
            break;
        case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
            execute(createDropColumnIfExistsStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNonNullDefaultValue()));
            break;
        case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
            //execute(createColumnIfNotExistsStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNonNullDefaultValue, "nvarchar(max)", "1"));
            break;
        case COPY_TABLE:
        case COPY_COLUMN_TO_SMALLER_DATATYPE:
        case RENAME_TABLE: // if this is the last operation, then there is no need to dp anything here. otherwise the following operations need to rename it back
        case CREATE_FULLTEXT_INDEX:
        case NUM_DDL:
        default:
            break;
        }
    }

    public abstract void prepareForDDLs(DDL_TYPE ddlOp);
    public abstract List<ExecutionStats> runChange(DDL_TYPE ddlOp);

    protected ExecutionStats runDDL(DDL_TYPE ddlOp)
    {
        if (ddlOp.getValue() >= DDL_TYPE.NUM_DDL.getValue()) {
            throw new RuntimeException("Invalid DDL Operation");
        }
        try {
            switch (ddlOp) {
                case ADD_COLUMN:
                    return execute(createColumnStatement(TableConstants.getUniqueColumnNameWithNullDefaultValue(), "Bit", null));
                case DROP_COLUMN:
                    // TODO: need to make sure that the columns with these names exist
                    return execute(dropColumnStatement(TableConstants.getUniqueColumnNameWithNullDefaultValue()));
                case ADD_COLUMN_WITH_CONSTRAINT:
                    return execute(createColumnStatement(TableConstants.getUniqueColumnNameWithNullDefaultValue(), "Bit", "0"));
                case RENAME_COLUMN:
                    String columnName = TableConstants.getUniqueColumnNameWithNullDefaultValue();
                    TableConstants.resetUniqueNameIdGeneratorByStep(1);
                    return execute(renameColumnStatement(columnName, TableConstants.getUniqueRenamedColumnName()));
                case MODIFY_COLUMN_INCREASE_STRING_DATATYPE:
                    return execute("ALTER TABLE " + tableName + " ALTER COLUMN " + TableConstants.getUniqueName("name") + " nvarchar(255) WITH (ONLINE = ON)");
                case MODIFY_COLUMN_SHRINK_STRING_DATATYPE:
                    return execute("ALTER TABLE " + tableName + " ALTER COLUMN " + TableConstants.getUniqueName("email") + " nvarchar(64) WITH (ONLINE = ON)");

                case CREATE_IDX:
                    return execute(createIndexStatement(false, false));
                case CREATE_IDX_ONLINE:
                    return execute(createIndexStatement(true, false));
                case REBUILD_IDX:
                    return execute(rebuildIndex(false));
                case REBUILD_IDX_ONLINE:
                    return execute(rebuildIndex(true));
                case DROP_IDX:
                    return execute(dropIndex(false));

                case ADD_CLUSTERED_INDEX:
                    return execute(createIndexStatement(false, true));
                case DROP_CLUSTERED_INDEX:
                    return execute(dropClusteredIndex(false));
                case ADD_CLUSTERED_INDEX_ONLINE:
                    return execute(createIndexStatement(true, true));
                case DROP_CLUSTERED_INDEX_ONLINE:
                    return execute(dropClusteredIndex(true));

                case ADD_CONSTRAINT:
                    return execute(createUniqueConstraintStatement());
                case DROP_CONSTRAINT:
                    return execute(dropUniqueConstraintStatement());

                case COPY_TABLE:
                    return execute("SELECT *  INTO users_copy FROM  " + tableName);
                case COPY_COLUMN_TO_THE_SAME_DATATYPE:
                    return execute("update " + tableName + " set name_copy = name");
                case COPY_COLUMN_TO_SMALLER_DATATYPE:
                    return execute("update " + tableName + " set name_copy = email");
                case COPY_COLUMN_TO_LARGER_DATATYPE:
                    return execute("update " + tableName + " set email_copy = name");
                case RENAME_TABLE:
                    return execute("EXEC sp_rename '" + tableName + "', 'users_renamed';");

                case ADD_COLUMN_DEFAULT_VALUE_NOT_NULL:
                    return execute(createColumnStatement(TableConstants.getUniqueColumnNameWithNonNullDefaultValue(), "nvarchar(8)", "bla"));
                case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
                    return execute(createColumnStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNullDefaultValue(), "nvarchar(max)", null));
                case ADD_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
                    return execute(createColumnStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNonNullDefaultValue(), "nvarchar(max)", "blablabla"));

                case DROP_COLUMN_DEFAULT_VALUE_NOT_NULL:
                    return execute(dropColumnStatement(TableConstants.getUniqueColumnNameWithNonNullDefaultValue()));
                case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NULL:
                    return execute(dropColumnStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNullDefaultValue()));
                case DROP_COLUMN_NVARCHAR_MAX_DEFAULT_NOT_NULL:
                    return execute(dropColumnStatement(TableConstants.getUniqueColumnNameWithMaxCharTypeNonNullDefaultValue()));

                case CREATE_FULLTEXT_INDEX:
                    return execute(createFullTextIndexStatement());

                case NUM_DDL:
                default:
                    break;
            }
            throw new RuntimeException("Invalid DDL_TYPE object");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private ExecutionStats execute(String query)
    {
        System.out.println(query);
        ExecutionStats executionStats = null;
        Connection connection = getConnection();
        try (Statement statement = connection.createStatement()) {
            Date start = new Date();
            statement.execute(query);
            Date end = new Date();
            executionStats = new ExecutionStats(start, end, 0);
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
        createConstraintStr.append("ALTER TABLE " + tableName);
        createConstraintStr.append(System.lineSeparator());
        createConstraintStr.append("ADD CONSTRAINT C_user UNIQUE (id, name)");
        return createConstraintStr.toString();
    }

    private String dropUniqueConstraintIfExistsStatement()  {
        StringBuilder dropConstraintStr = new StringBuilder();
        dropConstraintStr.append("ALTER TABLE " + tableName);
        dropConstraintStr.append(System.lineSeparator());
        dropConstraintStr.append("DROP CONSTRAINT IF EXISTS C_user");
        dropConstraintStr.append(System.lineSeparator());
        return dropConstraintStr.toString();
    }

    private String dropUniqueConstraintStatement()
    {
        StringBuilder dropConstraintStr = new StringBuilder();
        dropConstraintStr.append("ALTER TABLE " + tableName);
        dropConstraintStr.append(System.lineSeparator());
        dropConstraintStr.append("DROP CONSTRAINT C_user");
        dropConstraintStr.append(System.lineSeparator());
        return dropConstraintStr.toString();
    }

    private java.lang.String createIndexIfNotExistsStatement(boolean isClustered) {
        StringBuilder createIndexStr = new StringBuilder();
        createIndexStr.append("IF NOT EXISTS (SELECT name from sys.indexes where name = '");
        createIndexStr.append(TableConstants.getUniqueIndexName());
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
        createIndexStr.append(TableConstants.getUniqueIndexName());
        createIndexStr.append("] ON " + tableName + " ([name] ASC, [email] ASC) ");
        if (isOnline) {
            createIndexStr.append("WITH (ONLINE=ON)");
        }
        createIndexStr.append(" ON [PRIMARY]");
        return createIndexStr.toString();
    }

    private String dropIndexIfExists() {
        return "DROP INDEX IF EXISTS [" + TableConstants.getUniqueIndexName() + "] on " + tableName;
    }

    private String dropClusteredIndexIfExists() {
        //declare @pki_name nvarchar(255)
        //set @pki_name = (select name from sys.key_constraints where name like '%users%')
        //print @pki_name
        //        EXECUTE ('ALTER TABLE users DROP CONSTRAINT IF EXISTS [' + @pki_name + ']')
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("declare @pki_name nvarchar(255)");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("set @pki_name = (select name from sys.indexes where name like '%" + tableName + "%')");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("IF @pki_name <> ''");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("EXECUTE ('DROP INDEX [' + @pki_name + '] on " + tableName + "')");
        return strBuilder.toString();
    }

    private String dropClusteredIndex(boolean isOnline)
    {
        StringBuilder strBuilder = new StringBuilder();
        //strBuilder.append("ALTER TABLE users DROP CONSTRAINT [");
        strBuilder.append("DROP INDEX [");
        strBuilder.append(TableConstants.getUniqueIndexName());
        strBuilder.append("]");
        strBuilder.append(" on " + tableName);
        if (isOnline) {
            strBuilder.append(" WITH (ONLINE = ON)");
        }
        return strBuilder.toString();
    }

    private String rebuildIndex(boolean isOnline)
    {
        String rebuildIndex = "ALTER INDEX " + TableConstants.getUniqueIndexName() + " on " + tableName + " REBUILD";
        if (isOnline) {
            rebuildIndex += " WITH (ONLINE = ON);";
        }
        return rebuildIndex;
    }

    private String dropIndex(boolean isOnline)
    {
        String dropIndexStr = "DROP INDEX [" + TableConstants.getUniqueIndexName() + "] on " + tableName;
        if (isOnline) {
            dropIndexStr += " WITH (ONLINE = ON)";
        }
        return dropIndexStr;
    }

    private String renameColumnStatement(String oldName, String newName)
    {
        return "exec sp_rename '" + tableName + "." + oldName + "', '" + newName + "', 'COLUMN';";

    }

    private String dropColumnStatement(String columnName) {
        return "ALTER TABLE " + tableName + " DROP COLUMN " + columnName;
    }

    private String createDropColumnIfExistsStatement(String columnName) {
        return "ALTER TABLE " + tableName + " DROP COLUMN IF EXISTS " + columnName;
    }

    private String createDropConstraintForColumnIfExistsStatement(String columnName) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("declare @Command  nvarchar(1000)");
        strBuilder.append("select @Command = 'ALTER TABLE " + this.tableName + " DROP CONSTRAINT ' + d.name");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("from sys.tables t\n" +
                "  join sys.default_constraints d on d.parent_object_id = t.object_id");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("join sys.columns c on c.object_id = t.object_id and c.column_id = d.parent_column_id");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("where t.name = '" + tableName + "'");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("and t.schema_id = schema_id('dbo') ");
        strBuilder.append("and c.name = '" + columnName + "'");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("IF @Command <> '' execute (@Command)");
        return strBuilder.toString();
    }

    private String createColumnStatement(String columnName, String datatype, String defaultValue) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append("ALTER TABLE " + tableName + " ADD ");
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
        strBuilder.append("SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + tableName + "'");
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
        strBuilder.append("CREATE FULLTEXT INDEX ON " + tableName + "(email)");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("KEY INDEX UQ_id");
        strBuilder.append(System.lineSeparator());
        strBuilder.append("WITH STOPLIST = SYSTEM");
        return strBuilder.toString();
    }

}
