package io.quantumdb.demo.utils;

public class TableConstants {
    public static class UniqueIdGenerator
    {
        private static int id = 0;

        public static int next() {
            return id++;
        }

        public static void reset() {
            id = 0;
        }
    }

    public static final String COLUMN_NAME_WITH_NULL_DEFAULT_VALUE = "is_activated";
    public static final String COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE = "non_null_string";
    public static final String COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE = "nvarchar_max_string_default_null";
    public static final String COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE = "nvarchar_max_string_default_non_null";
    public static final String RENAMED_COLUMN_NAME = "renamed_is_activated";
    public static final String INDEX_NAME = "I_users1";

    public static String getUniqueColumnNameWithNullDefaultValue()
    {
        return COLUMN_NAME_WITH_NULL_DEFAULT_VALUE + UniqueIdGenerator.next();
    }

    public static String getUniqueColumnNameWithNonNullDefaultValue()
    {
        return COLUMN_NAME_WITH_NON_NULL_DEFAULT_VALUE + UniqueIdGenerator.next();
    }

    public static String getUniqueColumnNameWithMaxCharTypeNullDefaultValue()
    {
        return COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NULL_DEFAULT_VALUE + UniqueIdGenerator.next();
    }

    public static String getUniqueColumnNameWithMaxCharTypeNonNullDefaultValue()
    {
        return COLUMN_NAME_WITH_MAX_NVARCHAR_TYPE_NON_NULL_DEFAULT_VALUE + UniqueIdGenerator.next();
    }

    public static String getUniqueRenamedColumnName()
    {
        return RENAMED_COLUMN_NAME + UniqueIdGenerator.next();
    }

    public static String getUniqueIndexName()
    {
        return INDEX_NAME + UniqueIdGenerator.next();
    }

    public static void resetUniqueNameIdGenerator() {
        UniqueIdGenerator.reset();
    }
}
