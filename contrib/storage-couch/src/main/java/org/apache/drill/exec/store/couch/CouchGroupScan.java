/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.couch.CouchSubScan.CouchSubScanSpec;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;

@JsonTypeName("couch-scan")
public class CouchGroupScan extends AbstractGroupScan {

    private static final Integer select = Integer.valueOf(1);

    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchGroupScan.class);

    private static final Comparator<List<CouchSubScanSpec>> LIST_SIZE_COMPARATOR = new Comparator<List<CouchSubScanSpec>>() {
        @Override
        public int compare(List<CouchSubScanSpec> list1,
                           List<CouchSubScanSpec> list2) {
            return list1.size() - list2.size();
        }
    };

    private static final Comparator<List<CouchSubScanSpec>> LIST_SIZE_COMPARATOR_REV = Collections
            .reverseOrder(LIST_SIZE_COMPARATOR);

    private CouchStoragePlugin storagePlugin;

    private CouchStoragePluginConfig storagePluginConfig;

    private CouchScanSpec scanSpec;

    private List<SchemaPath> columns;

    private Map<Integer, List<CouchSubScanSpec>> endpointFragmentMapping;

    private boolean filterPushedDown = false;

    private CouchSubScanSpec subScanSpec;

    @JsonCreator
    public CouchGroupScan(
            @JsonProperty("userName") String userName,
            @JsonProperty("couchScanSpec") CouchScanSpec scanSpec,
            @JsonProperty("storage") CouchStoragePluginConfig storagePluginConfig,
            @JsonProperty("columns") List<SchemaPath> columns,
            @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException,
            ExecutionSetupException {
        this(userName, (CouchStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig),
                scanSpec, columns);
    }

    public CouchGroupScan(String userName, CouchStoragePlugin storagePlugin,
                          CouchScanSpec scanSpec, List<SchemaPath> columns) throws IOException {
        super(userName);
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.scanSpec = scanSpec;
        this.columns = columns;
        this.storagePluginConfig.getConnection();
        logger.info("group scan 1 {}", columns);

    }

    private CouchGroupScan(CouchGroupScan that) {
        super(that);
        this.scanSpec = that.scanSpec;
        this.columns = that.columns;
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePluginConfig;
        this.endpointFragmentMapping = that.endpointFragmentMapping;
        this.filterPushedDown = that.filterPushedDown;
        logger.info("group scan 2 {}", that.columns);
    }



    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }
    @JsonIgnore
    public void setFilterPushedDown(boolean filterPushedDown) {
        this.filterPushedDown = filterPushedDown;
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        CouchGroupScan clone = new CouchGroupScan(this);
        clone.columns = columns;
        logger.info("group scan 3 {}", columns);
        // selection columns from here
        logger.debug("CouchGroupScan clone {}", columns);
        return clone;
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @Override
    public void applyAssignments(List<DrillbitEndpoint> endpoints)
            throws PhysicalOperatorSetupException {
        subScanSpec = buildSubScanSpecAndGet();
    }

    public CouchSubScanSpec buildSubScanSpecAndGet(){
        logger.info("buildSubScanSpecAndGet");
        CouchSubScanSpec subScanSpec = new CouchSubScanSpec()
                .setDbName(scanSpec.getDbName())
                .setTableName(scanSpec.getTableName())
                .setHost("http://localhost:5984")
                .setFilter(scanSpec.getFilters());
        return subScanSpec;
    }

    @Override
    public CouchSubScan getSpecificScan(int minorFragmentId) { // pass to HttpScanBatchCreator
        logger.debug("CouchGroupScan getSpecificScan");
        return new CouchSubScan(getUserName(), storagePlugin, storagePluginConfig,subScanSpec, columns);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 0;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public ScanStats getScanStats() {
        return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, 1, 1, (float) 10);
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new CouchGroupScan(this);
    }

    @Override
    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty("couchScanSpec")
    public CouchScanSpec getScanSpec() {
        return scanSpec;
    }

    @JsonProperty("storage")
    public CouchStoragePluginConfig getStorageConfig() {
        return storagePluginConfig;
    }

    @JsonIgnore
    public CouchStoragePlugin getStoragePlugin() {
        return storagePlugin;
    }

    @Override
    public String toString() {
        return "CouchGroupScan [CouchGroupScan=" + scanSpec + ", columns=" + columns
                + "]";
    }
}