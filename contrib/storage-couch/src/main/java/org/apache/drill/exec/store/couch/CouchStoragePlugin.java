package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.util.Set;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.drill.exec.store.couch.schema.CouchSchemaFactory;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CouchStoragePlugin extends AbstractStoragePlugin {
    static final Logger logger = LoggerFactory
            .getLogger(CouchStoragePlugin.class);

    private DrillbitContext context;
    private CouchStoragePluginConfig couchConfig;
    private CouchSchemaFactory schemaFactory;

    public CouchStoragePlugin(CouchStoragePluginConfig couchConfig,
                              DrillbitContext context, String name) throws IOException,
            ExecutionSetupException {
        super(context, name);
        logger.debug("initialize CouchStoragePlugin {} {}", name, couchConfig);
        this.context = context;
        this.couchConfig = couchConfig;
        this.schemaFactory = new CouchSchemaFactory(this, name);
    }

    public DrillbitContext getContext() {
        return this.context;
    }

    @Override
    public CouchStoragePluginConfig getConfig() {
        return couchConfig;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        /* selection only represent database and collection name */
        CouchScanSpec spec = selection.getListWith(new ObjectMapper(), new TypeReference<CouchScanSpec>() {});
        logger.debug("getPhysicalScan {} {} {} {}", userName, selection, selection.getRoot(), spec);
        return new CouchGroupScan(userName, this, spec,null);
    }
    @Override
    public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(CouchPushDownFilterForScan.INSTANCE);
    }

}