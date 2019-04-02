package org.apache.drill.exec.store.couch;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

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
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;

import static org.apache.drill.common.expression.SchemaPath.STAR_COLUMN;

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


    public CouchRecordReader(FragmentContext context,List<SchemaPath> projectedColumns, CouchSubScan.CouchSubScanSpec subScanSpec, CouchStoragePlugin plugin) {
        fields = new JSONObject();
        // exclude _id field, if not mentioned by user.
        //fields.put(, Integer.valueOf(0));
        setColumns(projectedColumns);
        fragmentContext = context;
        this.plugin = plugin;
        filters = new JSONObject();
        //Map<String, List<JSONObject>> mergedFilters = CouchUtils.mergeFilters(
        //        subScanSpec.getMinFilters(), subScanSpec.getMaxFilters());
//
        //buildFilters(subScanSpec.getFilter(), mergedFilters);
        logger.debug("BsonRecordReader is enabled? " + isBsonRecordReader);
        tableName = subScanSpec.TableName;
        HttpClient httpClient = null;
        try {
            httpClient = new StdHttpClient.Builder()
                    .url("http://localhost:5984/")
                    .build();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        connection = new StdCouchDbConnector(subScanSpec.getTableName(),dbInstance);

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

        //加参数以识别column
        //loadCouch();
    }

    private void loadCouch() {
        Map map = new HashMap();
        String dataPost;
        String url;
        url="http://localhost:5984/hello/_all_docs?include_docs=true";


        //Map<String,Object> args = subScan.getFullURL().get(url);
        //filters = subScan.getScanSpec().getFilters();
        SimpleHttp http = new SimpleHttp();
        String result = http.get(url);
        logger.debug("http '{}' response {} bytes", url, result.length());
        JSONObject res = JSONObject.fromObject(result);
        JSONArray rows = res.getJSONArray("rows");
        for(int i=0;i<rows.size();i++){
            JSONObject job = rows.getJSONObject(i);  // 遍历 jsonarray 数组，把每一个对象转成 json 对象
            String doc = job.get("doc").toString();
            logger.debug(job.toString());
        }

        //parseResult(content);
    }

    @Override
    public int next() {
        if(jsonIt == null){
                ViewQuery q = new ViewQuery()
                        .allDocs()
                        .includeDocs(true);
                Iterator<ViewResult.Row> resultIterator = connection.queryView(q).iterator();
                Set<JSONObject> stringSet = new HashSet<>();

                while (resultIterator.hasNext()){
                    stringSet.add(JSONObject.fromObject(resultIterator.next().getDoc()));
                }
                jsonIt = stringSet.iterator();
        }

        logger.debug("CouchRecordReader next");
        if (jsonIt == null || !jsonIt.hasNext()) {
            return 0;
        }
        writer.allocate();
        writer.reset();
        int docCount = 0;
        try {
            while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && jsonIt.hasNext()) {
                JSONObject node = JSONObject.fromObject(jsonIt.next());
                logger.debug(node.toString() + "jsonnode");
                jsonReader.setSource(node.toString().getBytes(Charsets.UTF_8));
                writer.setPosition(docCount);
                jsonReader.write(writer);
                docCount ++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        writer.setValueCount(docCount);
        return docCount;
    }

    @Override
    public void close() {
        logger.debug("CouchRecordReader cleanup");
    }

}
