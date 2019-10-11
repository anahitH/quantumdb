package io.quantumdb.demo.utils;

import java.util.ArrayList;
import java.util.List;


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
                enumToInt.add(CREATE_IDX_ONLINE.getValue(), CREATE_IDX_ONLINE);
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
                    return "RebuildIndexOnline";
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
