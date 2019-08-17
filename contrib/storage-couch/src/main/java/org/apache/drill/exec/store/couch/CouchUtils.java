package org.apache.drill.exec.store.couch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import net.sf.json.JSONObject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CouchUtils {
    static final Logger logger = LoggerFactory.getLogger(CouchRecordReader.class);
    public static JSONObject andFilterAtIndex(JSONObject leftFilter,
                                              JSONObject rightFilter) {
        JSONObject andQueryFilter = new JSONObject();
        List<JSONObject> filters = new ArrayList<JSONObject>();
        filters.add(leftFilter);
        filters.add(rightFilter);
        andQueryFilter.put("$and", filters);
        return andQueryFilter;
    }

    public static JSONObject orFilterAtIndex(JSONObject leftFilter,
                                             JSONObject rightFilter) {
        JSONObject orQueryFilter = new JSONObject();
        List<JSONObject> filters = new ArrayList<JSONObject>();
        filters.add(leftFilter);
        filters.add(rightFilter);
        orQueryFilter.put("$or", filters);
        return orQueryFilter;
    }

    public static Map<String, List<JSONObject>> mergeFilters(
            Map<String, Object> minFilters, Map<String, Object> maxFilters) {
        Map<String, List<JSONObject>> filters = Maps.newHashMap();
        logger.info("what t fk????????????????");
        for (Entry<String, Object> entry : minFilters.entrySet()) {
            List<JSONObject> list = filters.get(entry.getKey());
            if (list == null) {
                list = Lists.newArrayList();
                filters.put(entry.getKey(), list);
            }
            JSONObject res1 = new JSONObject();
            JSONObject res2 = new JSONObject();
            res1.put(entry.getKey(),res2.put("$gte",entry.getValue()));
            list.add(res1);
        }

        for (Entry<String, Object> entry : maxFilters.entrySet()) {
            List<JSONObject> list = filters.get(entry.getKey());
            if (list == null) {
                list = Lists.newArrayList();
                filters.put(entry.getKey(), list);
            }
            JSONObject res1 = new JSONObject();
            JSONObject res2 = new JSONObject();
            res1.put(entry.getKey(),res2.put("$lt",entry.getValue()));
            list.add(res1);
        }
        return filters;
    }
}
