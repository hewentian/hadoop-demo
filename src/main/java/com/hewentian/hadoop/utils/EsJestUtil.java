package com.hewentian.hadoop.utils;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hewentian.hadoop.entity.EsUser;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.*;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.DeleteMapping;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * <p>
 * <b>EsJestUtil</b> 是 EsJestUtil 工具类
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-09-18 4:25:01 PM
 * @since JDK 1.8
 */
public class EsJestUtil {
    private static Logger log = Logger.getLogger(EsJestUtil.class);

    private static JestClientFactory jestClientFactory;

    private static JestClient jestClient;

    private static String esHosts;

    static {
        esHosts = Config.get("es.cluster.hosts", null);

        if (StringUtils.isBlank(esHosts)) {
            log.error("请设置 es.cluster.hosts");
            System.exit(1);
        }

        List<String> serverUris = new ArrayList<String>();
//        serverUris.add("http://127.0.0.1:9200"); // 单节点时

        // serverUris.add("http://127.0.0.1:9201"); // 集群时
        // serverUris.add("http://127.0.0.1:9202");
        // serverUris.add("http://127.0.0.1:9203");
        for (String uri : esHosts.split(",")) {
            serverUris.add(uri);
        }

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder(serverUris).maxTotalConnection(5)
                .multiThreaded(true).connTimeout(30000).readTimeout(60000).build();

        jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        jestClient = jestClientFactory.getObject();
    }

    public static JestClient getJestClient() {
        return jestClientFactory.getObject();
    }

    public static boolean createIndex(String indexName, Map<String, Object> settings) throws Exception {
        // 首先判断是否已经存在该索引
        JestResult jr = jestClient.execute(new IndicesExists.Builder(indexName).build());

        if (jr.isSucceeded()) {
            return false;
        }

        CreateIndex createIndex = new CreateIndex.Builder(indexName).settings(settings).build();
        jr = jestClient.execute(createIndex);

        return jr.isSucceeded();
    }

    public static boolean deleteIndex(String indexName) {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();

        try {
            JestResult jr = jestClient.execute(deleteIndex);
            return jr.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static boolean putMappings(String indexName, String type, Object mappings) {
        PutMapping putMapping = new PutMapping.Builder(indexName, type, mappings).build();

        try {
            JestResult jr = jestClient.execute(putMapping);
            return jr.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 好像删不掉
     */
    @Deprecated
    public static boolean deleteMapping(String indexName, String type) {
        DeleteMapping deleteMapping = new DeleteMapping.Builder(indexName, type).build();

        try {
            JestResult jr = jestClient.execute(deleteMapping);
            return jr.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static JsonObject getMapping(String indexName, String type) {
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexName).addType(type).build();

        try {
            JestResult jr = jestClient.execute(getMapping);
            if (jr.isSucceeded()) {
                return jr.getJsonObject();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static boolean addDoc1(String indexName, String type, Object source, String id) {
        Index index = new Index.Builder(source).index(indexName).type(type).id(id).build();

        try {
            DocumentResult dr = jestClient.execute(index);

            return dr.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static boolean addDoc2(String indexName, String type, EsUser source) {
        Index index = new Index.Builder(source).index(indexName).type(type).refresh(true).build();

        try {
            DocumentResult dr = jestClient.execute(index);

            if (dr.isSucceeded()) {
                return true;
            } else {
                log.error(dr.getErrorMessage());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static JsonObject getDoc(String indexName, String id) {
        Get get = new Get.Builder(indexName, id).build();
        JsonObject jsonObject = null;

        try {
            DocumentResult dr = jestClient.execute(get);
            if (dr.isSucceeded()) {
                jsonObject = dr.getJsonObject();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return jsonObject;
    }

    /**
     * 这个方法可以部分更新，更好的方法请参见{link updateDoc(String, String, Object) updateDoc}
     */
    public static boolean updateDoc(String indexName, String typeName, String id, Map<String, Object> updateFields) {
        Get get = new Get.Builder(indexName, id).build();

        try {
            DocumentResult dr = jestClient.execute(get);
            if (!dr.isSucceeded()) {
                return false;
            }

            JsonObject jsonObject = dr.getJsonObject();
            JsonObject source = jsonObject.getAsJsonObject("_source");

            for (Entry<String, Object> en : updateFields.entrySet()) {
                String key = en.getKey();
                Object value = en.getValue();

                if (value instanceof String) {
                    source.addProperty(key, value.toString());
                } else if (value instanceof Boolean) {
                    source.addProperty(key, Boolean.valueOf(value.toString()));
                } else if (value instanceof Number) {
                    if (value instanceof Integer) {
                        source.addProperty(key, Integer.valueOf(value.toString()));
                    } else if (value instanceof Double) {
                        source.addProperty(key, Double.valueOf(value.toString()));
                    } else {
                        // do nothing
                    }
                } else if (value instanceof JsonElement) {
                    if (value instanceof JsonObject) {
                        source.add(key, (JsonObject) value);
                    } else if (value instanceof JsonArray) {
                        source.add(key, (JsonArray) value);
                    }
                } else {
                    // do nothing
                }
            }

            JsonObject json = new JsonObject();
            json.add("doc", source);
            Update update = new Update.Builder(json.toString()).index(indexName).type(typeName).id(id).build();
            dr = jestClient.execute(update);
            return dr.isSucceeded();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 脚本更新，推荐使用这种
     */
    public static boolean updateDoc(String indexName, String typeName, String id, Object updateScript) {
        try {
            Update update = new Update.Builder(updateScript).index(indexName).type(typeName).id(id).build();
            DocumentResult dr = jestClient.execute(update);

            if (dr.isSucceeded()) {
                return true;
            } else {
                log.error(dr.getErrorMessage());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 未实现
     **/
    @Deprecated
    public static boolean updateDoc(String indexName, String typeName, EsUser user) {
        try {
            Update update = new Update.Builder(user).index(indexName).type(typeName).build();
            DocumentResult dr = jestClient.execute(update);

            if (dr.isSucceeded()) {
                return true;
            } else {
                log.error(dr.getErrorMessage());
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static SearchResult search(String indexName, String typeName, Object query) {
        Search search = new Search.Builder(query.toString()).addIndex(indexName).addType(typeName).build();

        try {
            SearchResult sr = jestClient.execute(search);

            return sr;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return null;
    }

    public static boolean deleteDoc(String indexName, String typeName, String id) {
        Delete delete = new Delete.Builder(id).index(indexName).type(typeName).build();

        try {
            DocumentResult dr = jestClient.execute(delete);

            return dr.isSucceeded();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }

    public static boolean bulkUpdate(List<BulkableAction<DocumentResult>> actions) {
        Bulk bulk = new Bulk.Builder().addAction(actions).build();

        try {
            BulkResult br = jestClient.execute(bulk);

            log.info("FailedItems: " + br.getFailedItems().size());

            return br.isSucceeded();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }

        return false;
    }
}
