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
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import net.sf.json.JSONObject;
import org.lightcouch.CouchDbProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.lightcouch.CouchDbClient;

public class CouchRecordReader extends AbstractRecordReader {
    static final Logger logger = LoggerFactory.getLogger(CouchRecordReader.class);

    private String tableName;
    private CouchDbClient dbClient;
    private Iterator<JSONObject> jsonIt;

    private JsonReader jsonReader;
    private VectorContainerWriter writer;

    private JSONObject filters;
    private final List<String> fields;
    private JSONObject querystring;

    private final FragmentContext fragmentContext;

    private final CouchStoragePlugin plugin;

    private boolean isBsonRecordReader = false;

    public CouchRecordReader(CouchSubScan.CouchSubScanSpec subScanSpec, List<SchemaPath> projectedColumns
                             , FragmentContext context, CouchStoragePlugin plugin) {
        querystring = new JSONObject();
        fields = new ArrayList<>();

        logger.info("columns {}", projectedColumns);
        setColumns(projectedColumns);
        fragmentContext = context;
        this.plugin = plugin;
        filters = new JSONObject();
        filters = subScanSpec.getFilter();
        logger.info("filter {}, fields {}", filters, fields);
        querystring.put("selector", "{}");
        if(filters != null){
            querystring.put("selector", filters);
        }
        querystring.put("fields", fields);

        logger.debug("BsonRecordReader is enabled? " + isBsonRecordReader);
        tableName = subScanSpec.getTableName();
        CouchDbProperties properties = new CouchDbProperties()
                .setDbName(tableName)
                .setCreateDbIfNotExist(false)
                .setProtocol("http")
                .setHost("127.0.0.1")
                .setPort(5984)
                .setUsername("admin")
                .setPassword("admin");
        dbClient = new CouchDbClient(properties);
    }

    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
        Set<SchemaPath> transformed = Sets.newLinkedHashSet();
        if (!isStarQuery()) {
            for (SchemaPath column : projectedColumns) {
                String fieldName = column.getRootSegment().getPath();
                transformed.add(column);
                this.fields.add(fieldName);
            }
        } else {
            transformed.add(SchemaPath.STAR_COLUMN);
        }
        logger.info("transformed {}", transformed);
        return transformed;
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output)
            throws ExecutionSetupException {
        this.writer = new VectorContainerWriter(output);
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
                .schemaPathColumns(Lists.newArrayList(getColumns()))
                .allTextMode(true)
                .readNumbersAsDouble(false)
                .enableNanInf(true).build();
            logger.debug(" Intialized JsonRecordReader. " + getColumns().toString());
    }

    @Override
    public int next(){
        if(jsonIt == null){
            logger.info("filters: {}", querystring);
            jsonIt = dbClient.findDocs(querystring.toString(),JSONObject.class).iterator();
            //jsonIt = dbClient.findDocs("{\"selector\": {\"company\": \"sicnu.edu.cn\"}}",JSONObject.class).iterator();
        }

        logger.debug("CouchRecordReader next");
        if (jsonIt == null || !jsonIt.hasNext()) {
            return 0;
        }
        writer.allocate();
        writer.reset();
        int docCount = 0;
        Stopwatch watch = Stopwatch.createStarted();
        try {
            while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonIt.hasNext()) {
                jsonReader.setSource(jsonIt.next().toString().getBytes(Charsets.UTF_8));
                writer.setPosition(docCount);
                jsonReader.write(writer);
                docCount ++;
            }

            jsonReader.ensureAtLeastOneField(writer);

            writer.setValueCount(docCount);
            logger.info("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
        } catch (IOException e) {
            String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
            logger.error(msg, e);
            throw new DrillRuntimeException(msg, e);
        }
        return docCount;
    }

    @Override
    public void close() {
    }

    @Override
    public String toString() {
        Object reader = jsonReader;
        return "MongoRecordReader[reader=" + reader + "]";
    }
}
