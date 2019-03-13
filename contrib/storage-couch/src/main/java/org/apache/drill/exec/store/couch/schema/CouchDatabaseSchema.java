
package org.apache.drill.exec.store.couch.schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fourspaces.couchdb.Session;
import com.google.common.collect.ImmutableList;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.couch.CouchScanSpec;
import org.apache.drill.exec.store.couch.CouchStoragePlugin;
import org.apache.calcite.schema.Table;

import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.couch.CouchStoragePluginConfig;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class CouchDatabaseSchema extends AbstractSchema {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
            .getLogger(CouchDatabaseSchema.class);
    private final CouchSchemaFactory.CouchSchema couchSchema;
    private final Set<String> tableNames;

    private final Map<String, DrillTable> drillTables = Maps.newHashMap();


    public CouchDatabaseSchema(List<String> tableList, CouchSchemaFactory.CouchSchema couchSchema, String name) {
        super(couchSchema.getSchemaPath(), name);
        this.couchSchema = couchSchema;
        this.tableNames = Sets.newHashSet(tableList);

    }

    @Override
    public Table getTable(String tableName) { // table name can be any of string
        if (!tableNames.contains(tableName)) { // table does not exist
            return null;
        }

        if (!drillTables.containsKey(tableName)) {
            drillTables.put(tableName, couchSchema.getDrillTable(this.name, tableName));
        }

        return drillTables.get(tableName);
    }

    @Override
    public Set<String> getTableNames() {
        return tableNames;
    }

    @Override
    public String getTypeName() {
        return CouchStoragePluginConfig.NAME;
    }


}

