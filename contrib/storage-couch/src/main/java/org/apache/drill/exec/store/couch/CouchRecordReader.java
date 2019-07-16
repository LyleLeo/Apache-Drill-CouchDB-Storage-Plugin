package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.drill.jdbc.Driver;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.couch.util.JsonConverter;
import org.apache.drill.exec.store.couch.util.SimpleHttp;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.lightcouch.CouchDbClient;

public class CouchRecordReader extends AbstractRecordReader {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchRecordReader.class);

    private CouchDbConnector connection;
    private boolean isBsonRecordReader = false;
    private String tableName;
    private VectorContainerWriter writer;
    private JsonReader jsonReader;
    private FragmentContext fragmentContext;
    private CouchSubScan subScan;
    private Iterator<JSONObject> jsonIt;
    private final JSONObject fields;
    private JSONObject filters;
    private final CouchStoragePlugin plugin;
    private Iterator<ViewResult.Row> rowIterator = null;



    public CouchRecordReader(FragmentContext context,List<SchemaPath> projectedColumns, CouchSubScan.CouchSubScanSpec subScanSpec, CouchStoragePlugin plugin) {
        fields = new JSONObject();
        setColumns(projectedColumns);
        fragmentContext = context;
        this.plugin = plugin;
        filters = new JSONObject();
        logger.debug("BsonRecordReader is enabled? " + isBsonRecordReader);
        tableName = subScanSpec.getTableName();


    }

    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
        logger.debug(projectedColumns.toString() + "transformColumns");
        Set<SchemaPath> transformed = org.apache.drill.shaded.guava.com.google.common.collect.Sets.newLinkedHashSet();
        if (!isStarQuery()) {
            for (SchemaPath column : projectedColumns) {
                String fieldName = column.getRootSegment().getPath();
                transformed.add(column);
                this.fields.put(fieldName, Integer.valueOf(1));
            }
        } else {
            // Tale all the fields including the _id
            transformed.add(SchemaPath.STAR_COLUMN);
        }
        logger.debug(String.valueOf(isStarQuery()));
        return transformed;
    }

    private void buildFilters(JSONObject pushdownFilters,
                              Map<String, List<JSONObject>> mergedFilters) {
        for (Map.Entry<String, List<JSONObject>> entry : mergedFilters.entrySet()) {
            List<JSONObject> list = entry.getValue();
            if (list.size() == 1) {
                this.filters.putAll(list.get(0));
            } else {
                JSONObject andQueryFilter = new JSONObject();
                andQueryFilter.put("$and", list);
                this.filters.putAll(andQueryFilter);
            }
        }
        if (pushdownFilters != null && !pushdownFilters.isEmpty()) {
            if (!mergedFilters.isEmpty()) {
                this.filters = CouchUtils.andFilterAtIndex(this.filters, pushdownFilters);
            } else {
                this.filters = pushdownFilters;
            }
        }
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output)
            throws ExecutionSetupException {
        //logger.debug("HttpRecordReader setup, query {}", subScan.getFullURL());
        this.writer = new VectorContainerWriter(output);
        this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
                .schemaPathColumns(Lists.newArrayList(getColumns()))
                .allTextMode(true)
                .readNumbersAsDouble(false)
                .enableNanInf(true).build();
        //JsonReader(fragmentContext.getManagedBuffer(),
        //Lists.newArrayList(getColumns(), true, false, true);
            logger.debug(" Intialized JsonRecordReader. " + getColumns().toString());
    }

    @Override
    public int next(){
        long couchdbcount = 0;
        if(rowIterator == null){
            HttpClient httpClient = null;
            try {
                httpClient = new StdHttpClient.Builder()
                        .url("http://localhost:5984/")
                        .build();
            } catch (Exception e) {
                e.printStackTrace();
            }

            CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
            connection = new StdCouchDbConnector(tableName,dbInstance);
            long time_before = System.currentTimeMillis();
            ViewQuery q = new ViewQuery()
                        .allDocs()
                        .includeDocs(true);
            rowIterator = connection.queryView(q).iterator();
            long time_after = System.currentTimeMillis();
            httpClient.shutdown();
            logger.info("took {} ms to get {} records from couchdb", time_after-time_before, couchdbcount);

        }
        long memory_setting = 0;
        //if(couchdbcount != 0){
        //    memory_setting = couchdbcount*1500;
        //    logger.info("couchdbcount = {}, memory_setting = {}",couchdbcount, memory_setting);
        //    try{
        //        execute_sql(st,"ALTER SYSTEM SET `planner.memory.max_query_memory_per_node` = " + memory_setting);
        //    }catch (Exception e){
        //        e.printStackTrace();
        //    }
        //}
        logger.info("ALTER SYSTEM SET `planner.memory.max_query_memory_per_node` = " + memory_setting);

        logger.debug("CouchRecordReader next");
        if (rowIterator == null || !rowIterator.hasNext()) {
            return 0;
        }
        writer.allocate();
        writer.reset();
        int docCount = 0;
        Stopwatch watch = Stopwatch.createStarted();
        try {
            while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && rowIterator.hasNext()) {
                JSONObject row = JSONObject.fromObject(rowIterator.next().getDoc());
                logger.debug(row.toString() + "jsonnode");
                jsonReader.setSource(row.toString().getBytes(Charsets.UTF_8));
                writer.setPosition(docCount);
                jsonReader.write(writer);
                docCount ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("get {} records", docCount);
        writer.setValueCount(docCount);
        logger.info("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
        return docCount;
    }

    @Override
    public void close() {
        logger.debug("CouchRecordReader cleanup");
    }
    private static void execute_sql(Statement st, String sql) throws Exception{
        st.executeQuery(sql);
    }

}
