package org.apache.drill.exec.store.couch;
import java.util.*;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import net.sf.json.JSONObject;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.physical.base.AbstractBase;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.PhysicalVisitor;
import org.apache.drill.exec.physical.base.SubScan;

import com.google.common.collect.Iterators;
import org.apache.drill.exec.store.StoragePluginRegistry;

public class CouchSubScan extends AbstractBase implements SubScan {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchSubScan.class);

    private CouchScanSpec scanSpec;
    //private CouchStoragePluginConfig config;
    private final CouchStoragePluginConfig config;
    @JsonIgnore
    private final CouchStoragePlugin plugin;
    private final List<SchemaPath> columns;
    private final CouchSubScanSpec subscanspec;
    @JsonCreator
    public CouchSubScan(
            @JacksonInject StoragePluginRegistry registry,
            @JsonProperty("userName") String userName,
            @JsonProperty("couchPluginConfig") StoragePluginConfig couchPluginConfig,
            @JsonProperty("subscanspec") CouchSubScanSpec subscanspec,
            @JsonProperty("columns") List<SchemaPath> columns)
            throws ExecutionSetupException {
        super(userName);
        this.columns = columns;
        this.config = (CouchStoragePluginConfig) couchPluginConfig;
        this.plugin = (CouchStoragePlugin) registry
                .getPlugin(couchPluginConfig);
        this.subscanspec = subscanspec;
    }

    //public CouchSubScan(CouchStoragePluginConfig config, CouchScanSpec spec) {
    //    this.scanSpec = spec;
    //    this.config = config;
    //}
    public CouchSubScan(String userName, CouchStoragePlugin storagePlugin,
                        CouchStoragePluginConfig storagePluginConfig,CouchSubScanSpec subscanspec, List<SchemaPath> columns) {
        super(userName);
        this.plugin = storagePlugin;
        this.config = storagePluginConfig;
        this.subscanspec = subscanspec;
        this.columns = columns;
    }

    public List<SchemaPath> getColumns() {
        return columns;
    }

    public CouchSubScanSpec getSubScanSpec() {
        return subscanspec;
    }

    CouchScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonIgnore
    public CouchStoragePlugin getCouchStoragePlugin() {
        return plugin;
    }


    CouchStoragePluginConfig getStorageConfig() {
        return config;
    }

    @Override
    public <T, X, E extends Throwable> T accept(
            PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
        return physicalVisitor.visitSubScan(this, value);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        return new CouchSubScan(getUserName(), plugin, config,subscanspec, columns);
    }

    @Override
    public int getOperatorType() {
        return 1009;
    }

    @Override
    public Iterator<PhysicalOperator> iterator() {
        return Iterators.emptyIterator();
    }

    public static class CouchSubScanSpec {

        protected String dbName;
        protected String TableName;
        protected String host;
        protected Map<String, Object> minFilters;
        protected Map<String, Object> maxFilters;

        protected JSONObject filter;

        @JsonCreator
        public CouchSubScanSpec(@JsonProperty("dbName") String dbName,
                                @JsonProperty("TableName") String TableName,
                                @JsonProperty("host") String host,
                                @JsonProperty("filters") JSONObject filters) {
            this.dbName = dbName;
            this.TableName = TableName;
            this.host = host;
            this.filter = filters;
        }

        CouchSubScanSpec() {

        }

        public String getDbName() {
            return dbName;
        }

        public CouchSubScanSpec setDbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public String getTableName() {
            return TableName;
        }

        public CouchSubScanSpec setTableName(String TableName) {
            this.TableName = TableName;
            return this;
        }

        public String getHost() {
            return host;
        }

        public CouchSubScanSpec setHost(String host) {
            this.host = host;
            return this;
        }

        public Map<String, Object> getMinFilters() {
            return minFilters;
        }

        public CouchSubScanSpec setMinFilters(Map<String, Object> minFilters) {
            this.minFilters = minFilters;
            return this;
        }

        public Map<String, Object> getMaxFilters() {
            return maxFilters;
        }

        public CouchSubScanSpec setMaxFilters(Map<String, Object> maxFilters) {
            this.maxFilters = maxFilters;
            return this;
        }

        public JSONObject getFilter() {
            return filter;
        }

        public CouchSubScanSpec setFilter(JSONObject filter) {
            this.filter = filter;
            return this;
        }

        @Override
        public String toString() {
            return "MongoSubScanSpec [dbName=" + dbName + ", collectionName="
                    + TableName + ", host=" + host + ", minFilters=" + minFilters
                    + ", maxFilters=" + maxFilters + ", filter=" + filter + "]";
        }

    }
}
