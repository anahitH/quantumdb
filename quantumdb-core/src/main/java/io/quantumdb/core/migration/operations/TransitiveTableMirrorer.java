package io.quantumdb.core.migration.operations;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.state.RefLog;
import io.quantumdb.core.state.RefLog.TableRef;
import io.quantumdb.core.utils.RandomHasher;
import io.quantumdb.core.versioning.TableMapping;
import io.quantumdb.core.versioning.Version;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class TransitiveTableMirrorer {

	static Set<String> mirror(Catalog catalog, RefLog refLog, Version version, String... tableNames) {
		refLog.prepareFork(version);
		Version parentVersion = version.getParent();

		Set<String> mirrored = Sets.newHashSet();

		// Copying tables which are transitively connected through foreign keys.
		List<String> tablesToMirror = Lists.newArrayList(tableNames);
		while(!tablesToMirror.isEmpty()) {
			String tableName = tablesToMirror.remove(0);
			TableRef tableRef = refLog.getTableRef(parentVersion, tableName);
			if (mirrored.contains(tableName)) {
				continue;
			}

			Table table = catalog.getTable(tableRef.getTableId());

			String newTableId = RandomHasher.generateTableId(refLog);
			tableMapping.ghost(version, tableName, newTableId);
			catalog.addTable(table.copy()
					.rename(newTableId));

			mirrored.add(tableName);

			// Traverse incoming foreign keys
			for (String referencingTableId : catalog.getTablesReferencingTable(tableId)) {
				tableMapping.getTableName(referencingTableId).ifPresent(tablesToMirror::add);
			}
		}

		// Copying foreign keys for each affected table.
		for(String tableName : mirrored) {
			String oldTableId = tableMapping.getTableId(parentVersion, tableName);
			String newTableId = tableMapping.getTableId(version, tableName);

			Table oldTable = catalog.getTable(oldTableId);
			Table newTable = catalog.getTable(newTableId);

			List<ForeignKey> outgoingForeignKeys = oldTable.getForeignKeys();
			for (ForeignKey foreignKey : outgoingForeignKeys) {
				String oldReferredTableId = foreignKey.getReferredTableName();
				String oldReferredTableName = tableMapping.getTableName(parentVersion, oldReferredTableId);
				String newReferredTableId = tableMapping.getTableId(version, oldReferredTableName);

				Table newReferredTable = catalog.getTable(newReferredTableId);
				String[] referencingColumnNames = foreignKey.getReferencingColumns().toArray(new String[0]);
				String[] referredColumnNames = foreignKey.getReferredColumns().toArray(new String[0]);

				newTable.addForeignKey(referencingColumnNames)
						.named(foreignKey.getForeignKeyName())
						.onUpdate(foreignKey.getOnUpdate())
						.onDelete(foreignKey.getOnDelete())
						.referencing(newReferredTable, referredColumnNames);
			}
		}

		return mirrored;
	}

}
