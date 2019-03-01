package com.hewentian.hadoop.es;

import com.alibaba.fastjson.JSONObject;
import com.hewentian.hadoop.utils.EsUtil;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * <b>EsDemo</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-27 17:53:53
 * @since JDK 1.8
 */
public class EsDemo {
    private static Logger log = Logger.getLogger(EsDemo.class);

    private static String indexName = "user_index";
    private static String typeName = "user";

    private static TransportClient transportClient;

    public static void addDocMap() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put("name", "张三");
        source.put("age", "23");
        List<String> tags = new ArrayList();
        tags.add("student");
        tags.add("programmer");
        source.put("tags", tags);
        source.put("birthday", "1989-06-30 11:22:33");

        IndexResponse indexResponse = transportClient.prepareIndex(indexName, typeName, UUID.randomUUID().toString())
                .setSource(source).execute().actionGet();
        System.out.println("add doc, id = " + indexResponse.getId());
    }

    public static void addDocXContentBuilder() throws Exception {
        List<String> tags = new ArrayList();
        tags.add("student");
        tags.add("programmer");

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().
                field("name", "李四")
                .field("age", "22")
                .field("tags", tags)
                .field("birthday", "1989-06-30 11:22:33")
                .endObject();

        IndexResponse indexResponse = transportClient.prepareIndex(indexName, typeName, UUID.randomUUID().toString())
                .setSource(builder).execute().actionGet();
        System.out.println("add doc, id = " + indexResponse.getId());
    }

    public static void addDocJson() throws Exception {
        JSONObject source = new JSONObject();
        source.put("name", "张三");
        source.put("age", "23");
        List<String> tags = new ArrayList();
        tags.add("student");
        tags.add("programmer");
        source.put("tags", tags);
        source.put("birthday", "1989-06-30 11:22:33");

        IndexResponse indexResponse = transportClient.prepareIndex(indexName, typeName)
                .setSource(source).execute().actionGet();
        System.out.println("add doc, id = " + indexResponse.getId());
    }

    public static void get1() {
        GetResponse response = transportClient.prepareGet(indexName, typeName, "3")
                .execute().actionGet();
        System.out.println("id: " + response.getId());
        System.out.println("source: " + response.getSourceAsString());
    }

    public static void query1() {
        QueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").gte(23);

        SearchResponse searchResponse = transportClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(rangeQueryBuilder)
                .addSort("age", SortOrder.ASC)
                .setSize(20)
                .execute()
                .actionGet();

        SearchHits hits = searchResponse.getHits();
        System.out.println("查到记录数：" + hits.getTotalHits());

        SearchHit[] searchHists = hits.getHits();
        if (searchHists.length > 0) {
            for (SearchHit hit : searchHists) {
                String id = hit.getId();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String name = sourceAsMap.get("name").toString();
                String age = sourceAsMap.get("age").toString();
                System.out.format("id: %s, name: %s , age: %s\n", id, name, age);
            }
        }
    }

    public static void deleteDoc() {
        DeleteResponse deleteResponse = transportClient.prepareDelete(indexName, typeName, "3").get();

        System.out.println(deleteResponse.status()); // OK
    }

    public static void updateDoc() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, "2");

        // 是部分更新，只会更新指定的字段：若存在的则修改，否则增加。
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Scott")
                .field("age", 80)
                .field("address", "Caliafornia, USA")
                .endObject();

        updateRequest.doc(builder);

        UpdateResponse updateResponse = transportClient.update(updateRequest).get();
        System.out.println(updateResponse.status()); // OK
    }

    public static void updateScript() throws ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, "2");

        Script script = new Script("ctx._source.gender=\"男\"");
        updateRequest.script(script);

        UpdateResponse updateResponse = transportClient.update(updateRequest).get();
        System.out.println(updateResponse.status()); // OK
    }

    /**
     * 当文档不存在的时候，就自动新增
     */
    public static void upsertDoc() throws IOException, ExecutionException, InterruptedException {
        String id = "3";
        IndexRequest indexRequest = new IndexRequest(indexName, typeName, id);
        XContentBuilder builder1 = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Tiger")
                .field("age", 70)
                .field("address", "Caliafornia, USA")
                .endObject();
        indexRequest.source(builder1);

        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id);

        // 是部分更新，只会更新指定的字段：若存在的则修改，否则增加。
        XContentBuilder builder2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("address", "NewYork, USA")
                .endObject();

        updateRequest.doc(builder2).upsert(indexRequest);

        UpdateResponse updateResponse = transportClient.update(updateRequest).get();
        System.out.println(updateResponse.status());
        // CREATED: 当id=3不存在的时候，会仅按build1的内容CREATED
        // OK: 而当id=3存在的时候则会按build2的内容更新
    }

    public static void scrollSearch() throws Exception {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.should(QueryBuilders.wildcardQuery("name", "*Ho"));
        boolQueryBuilder.should(QueryBuilders.termQuery("age", 23));

        SearchResponse searchResponse = transportClient.prepareSearch(indexName).setTypes(typeName)
                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
                .setQuery(boolQueryBuilder).setScroll(new TimeValue(60000))
                .setSize(100).get();

        long totalHits = searchResponse.getHits().totalHits;
        System.out.println("totalHits: " + totalHits);

        do {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                System.out.println("id = " + hit.getId());
            }

            String scrollId = searchResponse.getScrollId();
            searchResponse = transportClient.prepareSearchScroll(scrollId).setScroll(new TimeValue(60000)).execute().actionGet();
        } while (searchResponse.getHits().getHits().length != 0);

        System.out.println("end.");
    }

    /**
     * 从不同的index, type, 和id中获取
     */
    public static void multiGet() {
        MultiGetResponse multiGetResponse = transportClient.prepareMultiGet()
                .add(indexName, typeName, "1")
                .add(indexName, typeName, "2", "3", "4")
                .add("anotherIndex", "type", "foo")
                .get();

        for (MultiGetItemResponse itemResponse : multiGetResponse) {
            GetResponse response = itemResponse.getResponse();
            if (null != response && response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                System.out.println(sourceAsString);
            }
        }
    }

    /**
     * 可以一次执行新增或者删除等操作
     */
    public static void bulk() throws IOException {
        // 新增
        BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

        List<String> tags = new ArrayList();
        tags.add("student");
        tags.add("programmer");

        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().
                field("name", "李四之")
                .field("age", "22")
                .field("tags", tags)
                .field("birthday", "1989-06-30 11:22:33")
                .endObject();

        IndexRequestBuilder indexRequestBuilder = transportClient.prepareIndex(indexName, typeName).setSource(builder);

        bulkRequestBuilder.add(indexRequestBuilder);

        // 删除
        DeleteRequestBuilder deleteRequestBuilder = transportClient.prepareDelete(indexName, typeName, "3");
        bulkRequestBuilder.add(deleteRequestBuilder);

        // 更新
        // 是部分更新，只会更新指定的字段：若存在的则修改，否则增加。
        XContentBuilder builder2 = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Scott")
                .field("age", 80)
                .field("address", "Safrancisco, USA")
                .endObject();

        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, "2");
        updateRequest.doc(builder2);
        bulkRequestBuilder.add(updateRequest);

        BulkResponse response = bulkRequestBuilder.get();
        System.out.println("res: " + response.status());
    }

    public static void bulkProcessor() throws InterruptedException, IOException {
        BulkProcessor bulkProcessor = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                System.out.println("beforeBulk...");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                System.out.println("afterBulk...");
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                // 出错时执行
            }
        })
                .setBulkActions(10) // 10次请求执行一次
                .setBulkSize(new ByteSizeValue(1, ByteSizeUnit.MB)) // 1m的数据刷新一次bulk
                .setFlushInterval(TimeValue.timeValueSeconds(10)) // 固定10秒刷新一次
                .setConcurrentRequests(1) // 并发请求：0.不允许；1.允许
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3)) // 设置退避, 100ms后执行, 最大请求3次
                .build();

        // 添加请求
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("name", "Scott")
                .field("age", 80)
                .field("address", "Caliafornia, USA")
                .endObject();
        IndexRequest indexRequest = new IndexRequest(indexName, typeName, "3");
        indexRequest.source(builder);

        bulkProcessor.add(indexRequest);
        bulkProcessor.add(new DeleteRequest(indexName, typeName, "2"));

        bulkProcessor.awaitClose(2, TimeUnit.MINUTES);
//        bulkProcessor.close();
    }

    public static void aggregation() {
        SearchResponse response = transportClient.prepareSearch(indexName)
                .setTypes(typeName)
                .setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addAggregation(AggregationBuilders.terms("userCount").field("name"))
                .addAggregation(AggregationBuilders.terms("ageCount").field("age")).get();

        Aggregations aggregations = response.getAggregations();
        for (Aggregation a : aggregations) {
            System.out.println(a.getName() + ", " + a.getType() + " -------------------");

            if (a.getType().equals(StringTerms.NAME)) {
                StringTerms terms = (StringTerms) a;
                for (StringTerms.Bucket b : terms.getBuckets()) {
                    System.out.println(b.getKey() + ", " + b.getDocCount());
                }
            } else if (a.getType().equals(LongTerms.NAME)) {
                LongTerms terms = (LongTerms) a;
                for (LongTerms.Bucket b : terms.getBuckets()) {
                    System.out.println(b.getKey() + ", " + b.getDocCount());
                }
            }
        }

        // output example:~
//        ageCount, lterms -------------------
//        22, 2
//        23, 2
//        21, 1
//        80, 1
//        userCount, sterms -------------------
//        张三, 2
//        Scott, 1
//        Tim Ho, 1
//        李四, 1
//        李四之, 1
    }

    public static void filter() {
        SearchResponse response = transportClient.prepareSearch(indexName)
                .setTypes(typeName).setQuery(QueryBuilders.matchAllQuery())
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPostFilter(QueryBuilders.rangeQuery("age").gte(23).lte(100))
                .setExplain(true)// 根据数据相关度排序，和关键字匹配最高的排在前面
                .get();

        SearchHit[] hits = response.getHits().getHits();
        for (SearchHit h : hits) {
            System.out.println(h.getSourceAsMap());
        }

//        {birthday=1989-06-30 11:22:33, name=张三, age=23, tags=[student, programmer]}
//        {address=Caliafornia, USA, name=Scott, age=80}
//        {birthday=1989-06-30 11:22:33, name=张三, age=23, tags=[student, programmer]}
    }

    public static void main(String[] args) {
        try {
            transportClient = EsUtil.getClient();
//            EsUtil.connectInfo();
//            addDocMap();
//            addDocXContentBuilder();
//            addDocJson();
//            get1();
//            query1();
//            deleteDoc();
//            updateDoc();
//            updateScript();
//            upsertDoc();
//            scrollSearch();
//            multiGet();
//            bulk();
//            bulkProcessor();
//            aggregation();
//            filter();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            EsUtil.close();
        }
    }
}
