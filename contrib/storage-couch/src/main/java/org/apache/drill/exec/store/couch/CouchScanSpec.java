package org.apache.drill.exec.store.couch;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import net.sf.json.JSONObject;

public class CouchScanSpec {
    //private String uri;
    private Map<String, Object> args = new HashMap<String, Object>();
    private JSONObject filters;
    private String dbName;
    private String TableName;
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchRecordReader.class);

    @JsonCreator
    public CouchScanSpec(@JsonProperty("dbName") String dbName,
                         @JsonProperty("tableName") String tableName) {
        this.TableName = tableName;
        this.dbName = dbName;
    }

    public CouchScanSpec(String dbName, String TableName, JSONObject filters){
        this.dbName = dbName;
        this.TableName = TableName;
        this.filters = filters;
    }

    public CouchScanSpec(String TableName, String key, Object val,JSONObject filters) {
        this.TableName = TableName;
        this.args.put(key, val);//key是条件字段，value是条件字段的值
        this.filters = filters;
    }

    public String getTableName() {
        return TableName;
    }

    public JSONObject getFilters(){
        return filters;
    }

    public String getDbName() {
        return dbName;
    }
    @JsonIgnore
    public Map<String, Map<String,Object>> getURL() {
        Map<String,Map<String, Object>> res = new HashMap<String,Map<String, Object>>();
        logger.debug(args.toString());
        res.put(TableName, args);
        logger.debug(args.toString());

//        if (args.size() == 0) {
//            return uri;
//        }
//        Joiner j = Joiner.on('&');
//        String url = uri;
//        String argStr = j.withKeyValueSeparator("=").join(args);
//        if (url.endsWith("?")) {
//            url += argStr;
//        } else if (url.contains("?")) {
//            url += '&' + argStr;
//        } else {
//            url += '?' + argStr;
//        }
//        return url;
        return res;
    }

    public void merge(CouchScanSpec that) {
        for (Map.Entry<String, Object> entry : that.args.entrySet()) {
            this.args.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public String toString() {
        return "CouchScanSpec [tableName='" + TableName + "', args=" + args + "]";
    }
}


