package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.drill.exec.store.couch.CouchSubScan.CouchSubScanSpec;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;


public class CouchGroupScan extends AbstractGroupScan {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchGroupScan.class);

    private CouchScanSpec scanSpec;
    private CouchStoragePluginConfig config;
    private boolean filterPushedDown = false;
    private CouchSubScan.CouchSubScanSpec subScanSpec;

    private CouchStoragePlugin plugin;
    private List<SchemaPath> columns;

    private Stopwatch watch = Stopwatch.createUnstarted();

    private Map<Integer, List<CouchSubScanSpec>> endpointFragmentMapping;

    public CouchGroupScan(String userName, CouchStoragePluginConfig config, CouchScanSpec spec, List<SchemaPath> columns) {
        super(userName);
        scanSpec = spec;
        this.config = config;
        this.columns = columns;
    }

    public CouchGroupScan(String userName, CouchStoragePlugin storagePlugin,
                          CouchScanSpec scanSpec, List<SchemaPath> columns) throws IOException {
        super(userName);
        this.plugin = storagePlugin;
        this.config = storagePlugin.getConfig();
        this.scanSpec = scanSpec;
        this.columns = columns;
        this.config.getConnection();

    }

    public CouchGroupScan(CouchGroupScan that) {
        super(that);
        scanSpec = that.scanSpec;
        config = that.config;
        this.endpointFragmentMapping = that.endpointFragmentMapping;
    }

    public CouchScanSpec getScanSpec() {
        return scanSpec;
    }

    @Override
    public boolean canPushdownProjects(List<SchemaPath> columns) {
        return true;
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) { // pass to HttpScanBatchCreator
        logger.debug("CouchGroupScan getSpecificScan");
        return new CouchSubScan(getUserName(), plugin, config,subScanSpec, columns);
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 0;
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        // selection columns from here
        logger.debug("CouchGroupScan clone {}", columns);
        return this;
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @JsonProperty
    public List<SchemaPath> getColumns() {
        return columns;
    }

    //@Override
    //public List<EndpointAffinity> getOperatorAffinity() {
    //    watch.reset();
    //    watch.start();
    //    Map<String, DrillbitEndpoint> endpointMap = new HashMap<>();
    //    for (DrillbitEndpoint ep : plugin.getContext().getBits()) {
    //        endpointMap.put(ep.getAddress(), ep);
    //    }
//
    //    Map<DrillbitEndpoint, EndpointAffinity> affinityMap = new HashMap<>();
    //    DrillbitEndpoint ep = endpointMap.get("http://localhost:5984");
    //    if (ep != null) {
    //        EndpointAffinity affinity = affinityMap.get(ep);
    //        if (affinity == null) {
    //            affinityMap.put(ep, new EndpointAffinity(ep, 1));
    //        } else {
    //            affinity.addAffinity(1);
    //        }
    //    }
    //    logger.debug("Took {} µs to get operator affinity", watch.elapsed(TimeUnit.NANOSECONDS) / 1000);
    //    return new ArrayList<>(affinityMap.values());
    //}

    @Override
    public void applyAssignments(List<DrillbitEndpoint> endpoints)
            throws PhysicalOperatorSetupException {
        subScanSpec = buildSubScanSpecAndGet();
        logger.debug("HttpGroupScan applyAssignments");
    }

    public CouchSubScanSpec buildSubScanSpecAndGet(){
        CouchSubScanSpec subScanSpec = new CouchSubScanSpec()
                .setDbName(scanSpec.getDbName())
                .setTableName(scanSpec.getTableName())
                .setHost("http://localhost:5984")
                .setFilter(scanSpec.getFilters());
        return subScanSpec;
    }


    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
            throws ExecutionSetupException {
        logger.debug("CouchGroupScan getNewWithChildren");
        return new CouchGroupScan(this);
    }

    @Override
    public ScanStats getScanStats() {
        return new ScanStats(GroupScanProperty.EXACT_ROW_COUNT, 1, 1, (float) 10);
    }

    @JsonIgnore
    public boolean isFilterPushedDown() {
        return filterPushedDown;
    }

    public void setFilterPushedDown(boolean filterPushedDown) {
        this.filterPushedDown = filterPushedDown;
    }

    public CouchStoragePluginConfig getStorageConfig() {
        return config;
    }

}