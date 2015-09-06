package io.quantumdb.core.migration.operations;

import java.util.List;

import com.google.common.collect.Lists;
import io.quantumdb.core.migration.utils.DataMappings;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Index;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.DropColumn;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;

class DropColumnMigrator implements SchemaOperationMigrator<DropColumn> {

	@Override
	public void migrate(Catalog catalog, TableMapping tableMapping, DataMappings dataMappings, Version version,
			DropColumn operation) {
		String tableName = operation.getTableName();
		TransitiveTableMirrorer.mirror(catalog, tableMapping, version, tableName);

		String tableId = tableMapping.getTableId(version, tableName);
		Table table = catalog.getTable(tableId);
		dataMappings.copy(version)
				.drop(table, operation.getColumnName());

		table.removeColumn(operation.getColumnName());

		List<Index> indexes = Lists.newArrayList(table.getIndexes());
		for (Index index : indexes) {
			table.removeIndex(index.getColumns().toArray(new String[] {}));
		}
	}

}
