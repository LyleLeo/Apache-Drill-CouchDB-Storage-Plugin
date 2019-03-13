/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.couch.schema;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import net.sf.json.JSONObject;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.AbstractSchemaFactory;
import org.apache.drill.exec.store.couch.CouchScanSpec;
import org.apache.drill.exec.store.couch.CouchStoragePlugin;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.couch.CouchStoragePluginConfig;
import org.apache.drill.exec.store.couch.util.SimpleHttp;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheBuilder;
import org.apache.drill.shaded.guava.com.google.common.cache.CacheLoader;
import org.apache.drill.shaded.guava.com.google.common.cache.LoadingCache;
import org.apache.drill.shaded.guava.com.google.common.collect.ImmutableList;
import org.apache.drill.shaded.guava.com.google.common.collect.Maps;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;

public class CouchSchemaFactory extends AbstractSchemaFactory {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchSchemaFactory.class);

    private static final String DATABASES = "databases";

    private LoadingCache<String, List<String>> databases;
    private LoadingCache<String, List<String>> tableNameLoader;
    private final CouchStoragePlugin plugin;

    public CouchSchemaFactory(CouchStoragePlugin plugin, String schemaName) throws ExecutionSetupException {
        super(schemaName);
        this.plugin = plugin;

        databases = CacheBuilder //
                .newBuilder() //
                .expireAfterAccess(1, TimeUnit.MINUTES) //
                .build(new DatabaseLoader());

        tableNameLoader = CacheBuilder //
                .newBuilder() //
                .expireAfterAccess(1, TimeUnit.MINUTES) //
                .build(new TableNameLoader());
    }

    private class DatabaseLoader extends CacheLoader<String, List<String>> {

        @Override
        public List<String> load(String key) throws Exception {
            List<String> dbNames = new ArrayList<>();
            String connection = plugin.getConfig().getConnection();
            String url = connection + "/_all_dbs";
            logger.info(url);
            SimpleHttp http = new SimpleHttp();
            String s = http.get(url);
            s = s.substring(1,s.length()-1);
            String[] list = s.split(",");
            for(String i : list) {
                i = i.substring(1, i.length() - 1);
                if (!i.startsWith("_"))
                    dbNames.add(i);
            }
            return dbNames;
        }

    }

    private class TableNameLoader extends CacheLoader<String, List<String>> {

        @Override
        public List<String> load(String dbName) throws Exception {
            List<String> tableNames = new ArrayList<>();
            String connection = plugin.getConfig().getConnection();
            String url = connection + "/_all_dbs";
            logger.info(url);
            SimpleHttp http = new SimpleHttp();
            String s = http.get(url);
            s = s.substring(1,s.length()-1);
            String[] tables = s.split(",");
            for(String i : tables) {
                i = i.substring(1, i.length() - 1);
                if (!i.startsWith("_"))
                    tableNames.add(i);
            }
            return tableNames;
        }
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        CouchSchema schema = new CouchSchema(getName());
        SchemaPlus hPlus = parent.add(getName(), schema);
        schema.setHolder(hPlus);
    }

    class CouchSchema extends AbstractSchema {

        private final Map<String, CouchDatabaseSchema> schemaMap = Maps.newHashMap();

        public CouchSchema(String name) {
            super(ImmutableList.<String>of(), name);
        }

        @Override
        public AbstractSchema getSubSchema(String name) {
            List<String> tables;
            try {
                if (!schemaMap.containsKey(name)) {
                    tables = tableNameLoader.get(name);
                    schemaMap.put(name, new CouchDatabaseSchema(tables, this, name));
                }

                return schemaMap.get(name);

                //return new MongoDatabaseSchema(tables, this, name);
            } catch (ExecutionException e) {
                logger.warn("Failure while attempting to access MongoDataBase '{}'.",
                        name, e.getCause());
                return null;
            }

        }

        void setHolder(SchemaPlus plusOfThis) {
            for (String s : getSubSchemaNames()) {
                plusOfThis.add(s, getSubSchema(s));
            }
        }

        @Override
        public boolean showInInformationSchema() {
            return false;
        }

        @Override
        public Set<String> getSubSchemaNames() {
            try {
                List<String> dbs = databases.get(DATABASES);
                return Sets.newHashSet(dbs);
            } catch (ExecutionException e) {
                logger.warn("Failure while getting Couch database list.", e);
                return Collections.emptySet();
            }
        }

        List<String> getTableNames(String dbName) {
            try {
                return tableNameLoader.get(dbName);
            } catch (ExecutionException e) {
                logger.warn("Failure while loading table names for database '{}'.",
                        dbName, e.getCause());
                return Collections.emptyList();
            }
        }

        DrillTable getDrillTable(String dbName, String collectionName) {
            CouchScanSpec couchScanSpec = new CouchScanSpec(dbName, collectionName);
            return new DynamicDrillTable(plugin, getName(), null, couchScanSpec);
        }

        @Override
        public String getTypeName() {
            return CouchStoragePluginConfig.NAME;
        }
    }


}