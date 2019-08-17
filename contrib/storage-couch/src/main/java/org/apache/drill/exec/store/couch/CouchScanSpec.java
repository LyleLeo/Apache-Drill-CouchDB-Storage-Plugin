
package org.apache.drill.exec.store.couch;

import net.sf.json.JSONObject;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CouchScanSpec {
    private String dbName;
    private String TableName;

    private JSONObject filters;

    @JsonCreator
    public CouchScanSpec(@JsonProperty("dbName") String dbName,
                         @JsonProperty("tableName") String tableName) {
        this.dbName = dbName;
        this.TableName = tableName;
    }

    public CouchScanSpec(String dbName, String TableName, JSONObject filters) {
        this.dbName = dbName;
        this.TableName = TableName;
        this.filters = filters;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return TableName;
    }

    public JSONObject getFilters() {
        return filters;
    }

    @Override
    public String toString() {
        return "MongoScanSpec [dbName=" + dbName + ", collectionName="
                + TableName + ", filters=" + filters + "]";
    }

}


