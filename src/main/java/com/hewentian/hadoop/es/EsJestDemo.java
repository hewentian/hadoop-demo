package com.hewentian.hadoop.es;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.hewentian.hadoop.entity.EsUser;
import com.hewentian.hadoop.utils.EsJestUtil;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.*;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * <b>EsJestDemo</b> 是 {@linkplain com.hewentian.hadoop.utils.EsJestUtil EsJestUtil}
 * 工具类的测试
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-09-18 4:25:48 PM
 * @since JDK 1.8
 */
public class EsJestDemo {
    private static Logger log = Logger.getLogger(EsJestDemo.class);
    private static String indexName = "user_index";
    private static String typeName = "user";

    public static void createIndex() {
        try {
            Map<String, Object> settings = new HashMap<String, Object>();
            settings.put("number_of_shards", 4); // default is 5
            settings.put("number_of_replicas", 1); // default is 1

            boolean res = EsJestUtil.createIndex(indexName, settings);
            log.info("res: " + res);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static void deleteIndex() {
        boolean res = EsJestUtil.deleteIndex(indexName);
        log.info("res: " + res);
    }

    public static void putMappings() {
        Map<String, Object> mappings = new HashMap<String, Object>();
        Map<String, Object> properties = new HashMap<String, Object>();
        mappings.put("properties", properties);

        Map<String, String> idType = new HashMap<String, String>();
        idType.put("type", "long");
        idType.put("index", "false");
        properties.put("id", idType);

        Map<String, String> nameType = new HashMap<String, String>();
        nameType.put("type", "keyword");
        properties.put("name", nameType);

        Map<String, String> ageType = new HashMap<String, String>();
        ageType.put("type", "integer");
        properties.put("age", ageType);

        Map<String, String> tagsType = new HashMap<String, String>();
        tagsType.put("type", "keyword");
        tagsType.put("boost", "3.0");
        properties.put("tags", tagsType);

        Map<String, String> birthdayType = new HashMap<String, String>();
        birthdayType.put("type", "date");
        birthdayType.put("format", "strict_date_optional_time || epoch_millis || yyyy-MM-dd HH:mm:ss");
        properties.put("birthday", birthdayType);

        boolean res = EsJestUtil.putMappings(indexName, typeName, mappings);
        log.info("res: " + res);
    }

    public static void deleteMapping() {
        boolean res = EsJestUtil.deleteMapping(indexName, typeName);
        log.info("res: " + res);
    }

    public static void getMapping() {
        JsonObject res = EsJestUtil.getMapping(indexName, typeName);
        log.info("res: " + res);
    }

    /**
     * yyyy-MM-dd HH:mm:ss
     */
    public static void addDoc1() {
        JsonObject source = new JsonObject();
        source.addProperty("name", "Tim");
        source.addProperty("age", "23");
        JsonArray tags = new JsonArray();
        tags.add("student");
        tags.add("programmer");
        source.add("tags", tags);
        source.addProperty("birthday", "1989-06-30 11:22:33");

        boolean res = EsJestUtil.addDoc1(indexName, typeName, source, "1");
        log.info("res: " + res);
    }

    /**
     * timestamp
     */
    public static void addDoc2() {
        JsonObject source = new JsonObject();
        source.addProperty("name", "Tim");
        source.addProperty("age", "23");
        JsonArray tags = new JsonArray();
        tags.add("student");
        tags.add("programmer");
        source.add("tags", tags);
//        source.addProperty("birthday", "1989-06-30 11:22:33");

        try {
            source.addProperty("birthday", DateUtils.parseDate("1989-06-30 11:22:33", "yyyy-MM-dd HH:mm:ss").getTime());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        boolean res = EsJestUtil.addDoc1(indexName, typeName, source, "2");
        log.info("res: " + res);
    }

    /**
     * java.util.Date
     */
    public static void addDoc3() {
        EsUser user = new EsUser();
        user.setId(3L);
        user.setName("Tim Ho");
        user.setAge(23);
        user.setTags(new String[]{"student", "programmer", "president"});

        try {
            user.setBirthday(DateUtils.parseDate("1989-06-30 11:22:33", "yyyy-MM-dd HH:mm:ss"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        boolean res = EsJestUtil.addDoc2(indexName, typeName, user);
        log.info("res: " + res);
    }

    public static void getDoc() {
        JsonObject res = EsJestUtil.getDoc(indexName, "1");
        log.info("res: " + res);
    }

    public static void updateDoc1() {
        Map<String, Object> updateFields = new HashMap<String, Object>();
        updateFields.put("name", "Tim Ho");
        updateFields.put("age", 21);

        JsonArray tags = new JsonArray();
        tags.add("student");
        tags.add("programmer");
        tags.add("president");
        updateFields.put("tags", tags);

        boolean res = EsJestUtil.updateDoc(indexName, typeName, "1", updateFields);
        log.info("res: " + res);
    }

    public static void updateDoc2() {
        EsUser user = new EsUser();
        user.setId(3L);
        user.setName("Tim");
        user.setAge(21);
        user.setTags(new String[]{"student", "president"});

        try {
            user.setBirthday(DateUtils.parseDate("1989-06-30 11:23:33", "yyyy-MM-dd HH:mm:ss"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        boolean res = EsJestUtil.updateDoc(indexName, typeName, user);
        log.info("res: " + res);
    }

    public static void updateDoc3() {
        JsonObject updateScript = new JsonObject();
        updateScript.addProperty("script", "ctx._source.name=\"Tim\"");

        boolean res = EsJestUtil.updateDoc(indexName, typeName, "2", updateScript);
        log.info("res: " + res);
    }

    public static void search1() {
        JsonObject json = new JsonObject();
        JsonObject query = new JsonObject();
        JsonObject bool = new JsonObject();
        JsonArray must = new JsonArray();

        JsonObject mustField1 = new JsonObject();
        JsonObject name = new JsonObject();
        name.addProperty("name", "Tim Ho");
        mustField1.add("term", name);

        json.add("query", query);
        json.addProperty("from", 0);
        json.addProperty("size", 10);
        query.add("bool", bool);
        bool.add("must", must);
        must.add(mustField1);

        SearchResult searchResult = EsJestUtil.search(indexName, typeName, json);

        if (!searchResult.isSucceeded()) {
            return;
        }

        log.info("total:" + searchResult.getTotal());
        List<SearchResult.Hit<JsonObject, Void>> hits = searchResult.getHits(JsonObject.class, true);
        hits.forEach(h -> log.info(h.id + " " + h.source));
    }

    public static void search2() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 完全匹配
//         searchSourceBuilder.query(QueryBuilders.matchQuery("name", "Tim"));

        // 通配符查询
//        searchSourceBuilder.query(QueryBuilders.wildcardQuery("name", "Tim*"));

        // 多条件或查询
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
//        boolQueryBuilder.should(QueryBuilders.termQuery("name", "Tim"));
//        boolQueryBuilder.should(QueryBuilders.termQuery("age", 21));
//        searchSourceBuilder.query(boolQueryBuilder);

        // 多条件与查询
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("name", "Tim Ho"));
        boolQueryBuilder.must(QueryBuilders.termQuery("age", 21));
        searchSourceBuilder.query(boolQueryBuilder);

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        SearchResult searchResult = EsJestUtil.search(indexName, typeName, searchSourceBuilder);

        log.info("total:" + searchResult.getTotal());
        List<SearchResult.Hit<JsonObject, Void>> hits = searchResult.getHits(JsonObject.class, true);
        hits.forEach(h -> log.info(h.id + " " + h.source));
    }

    public static void deleteDoc() {
        boolean res = EsJestUtil.deleteDoc(indexName, typeName, "4");
        log.info("res: " + res);
    }

    public static void bulkUpdate() {
        JsonObject source = new JsonObject();
        source.addProperty("name", "Tim");
        source.addProperty("age", "23");
        JsonArray tags = new JsonArray();
        tags.add("student");
        tags.add("programmer");
        source.add("tags", tags);
        source.addProperty("birthday", "1989-06-30 11:22:33");

        JsonObject updateScript = new JsonObject();
        updateScript.addProperty("script", "ctx._source.name=\"Tim Ho\"");

        Index index1 = new Index.Builder(source).index(indexName).type(typeName).id("4").build();
        Index index2 = new Index.Builder(source).index(indexName).type(typeName).id("5").build();
        Delete delete = new Delete.Builder("1").index(indexName).type(typeName).build();
        Update update = new Update.Builder(updateScript).index(indexName).type(typeName).id("2").build();

        List<BulkableAction<DocumentResult>> actions = new ArrayList<BulkableAction<DocumentResult>>();
        actions.add(index1);
        actions.add(index2);
        actions.add(delete);
        actions.add(update);

        boolean res = EsJestUtil.bulkUpdate(actions);
        log.info("res: " + res);
    }

    public static void main(String[] args) throws Exception {
//        createIndex();
//        deleteIndex();
//        putMappings();
//        deleteMapping();
//        getMapping();
//        addDoc1();
//        addDoc2();
//        addDoc3();
//        getDoc();
//        updateDoc1();
//        updateDoc2();
//        updateDoc3();
//        Thread.sleep(2000); // 插入后，要等1到2秒才能查询出来
//        search1();
//        search2();
//        deleteDoc();
//        bulkUpdate();
//         http://www.xdemo.org/lucene4-8-ikanalyzer-springmvc4-jsoup-quartz/
    }
}
