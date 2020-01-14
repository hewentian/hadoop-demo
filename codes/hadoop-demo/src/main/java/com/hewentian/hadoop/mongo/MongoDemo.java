package com.hewentian.hadoop.mongo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hewentian.hadoop.entity.MongoUser;
import com.hewentian.hadoop.entity.MongoUserInfo;
import com.hewentian.hadoop.utils.CommonUtil;
import com.hewentian.hadoop.utils.MongoUtil;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.types.Binary;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * <p>
 * <b>MongoDemo</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-03-05 19:52:38
 * @since JDK 1.8
 */
public class MongoDemo {
    private static Logger log = Logger.getLogger(MongoDemo.class);

    private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static MongoDatabase mongoDatabase = MongoUtil.getMongoDatabase();
    private static MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("user");

    public static void insertOne() {
        Document doc = new Document();
        doc.put("_id", "100");
        doc.put("name", "张家100");
        doc.put("age", "100"); // 这里设置成string类型，方便下面根据type查询
        doc.put("region", "黄埔区");
        doc.put("address", "广州市黄埔区");

//        InsertOneOptions oneOptions = new InsertOneOptions().bypassDocumentValidation(true);
        mongoCollection.insertOne(doc);
    }

    /**
     * 批量插入数据
     */
    public static void bulkInsert() throws Exception {
        Document doc1 = new Document();
        doc1.put("_id", "1");
        doc1.put("name", "张三");
        doc1.put("age", 10);
        doc1.put("region", "天河区");
        doc1.put("address", "广州市天河区");
        doc1.put("birthday", "1989-01-02 11:22:33");
        doc1.put("createTime", format.parse("1989-01-02 11:22:33"));

        Document doc2 = new Document();
        doc2.put("_id", "2");
        doc2.put("name", "李四");
        doc2.put("age", 20);
        doc2.put("region", "番禺区");
        doc2.put("address", "广州市番禺区");
        doc2.put("birthday", "1989-01-02 11:32:33");
        doc2.put("createTime", format.parse("1989-01-02 11:32:33"));

        Document doc3 = new Document();
        doc3.put("_id", "3");
        doc3.put("name", "王五");
        doc3.put("age", 30);
        doc3.put("region", "黄埔区");
        doc3.put("address", "广州市黄埔区");
        doc3.put("birthday", "1993-01-02 11:22:33");
        doc3.put("createTime", format.parse("1993-01-02 11:22:33"));

        // 要批量插入的数据列表
        List<InsertOneModel<Document>> insertList = new ArrayList<InsertOneModel<Document>>();

        insertList.add(new InsertOneModel<Document>(doc1));
        insertList.add(new InsertOneModel<Document>(doc2));
        insertList.add(new InsertOneModel<Document>(doc3));

        for (int i = 4; i <= 10; i++) {
            Document doc = new Document();
            doc.put("_id", String.valueOf(i));
            doc.put("name", "张家" + i);
            doc.put("age", 20 + i);
            doc.put("region", "黄埔区");
            doc.put("address", "广州市黄埔区" + i);
            doc.put("birthday", (1990 + i) + "-01-02 11:22:33");
            doc.put("createTime", format.parse((1990 + i) + "-01-02 11:22:33"));
            insertList.add(new InsertOneModel<Document>(doc));
        }

        // 批量插入数据
        BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(insertList);
        int count = bulkWriteResult.getInsertedCount();
        log.info("---------- 插入 " + count + " 条数据成功");
    }

    public static void updateOne() {
        BasicDBObject filter = new BasicDBObject();
        filter.put("_id", "1");

        BasicDBObject update = new BasicDBObject();
        BasicDBObject set = new BasicDBObject();

        set.put("birthday", "1999-02-01");
        set.put("email", "zhansan@126.com");

        update.put("$set", set);

        UpdateOptions updateOptions = new UpdateOptions().upsert(true); // true: 数据不存在的时候会插入
        UpdateResult updateResult = mongoCollection.updateOne(filter, update, updateOptions);

        System.out.println("---------- 更新记录数： " + updateResult.getModifiedCount());
    }

    public static void bulkUpdate() {
        List<UpdateOneModel<Document>> updateList = new ArrayList<UpdateOneModel<Document>>();

        updateList.add(new UpdateOneModel<>(new BasicDBObject("_id", "1"),
                new BasicDBObject("$set", new BasicDBObject("email", "zs@126.com"))));

        updateList.add(new UpdateOneModel<>(new BasicDBObject("_id", "2"),
                new BasicDBObject("$set", new BasicDBObject("email", "ls@126.com").append("postCode", "10086"))));

        updateList.add(new UpdateOneModel<>(new BasicDBObject("_id", "3"),
                new BasicDBObject("$set", new BasicDBObject("age", 31))));

        // 批量更新数据
        BulkWriteResult bulkWriteResult = mongoCollection.bulkWrite(updateList);
        int count = bulkWriteResult.getModifiedCount();
        log.info("---------- 更新 " + count + " 条数据成功");
    }

    public static void deleteMany() {
        JSONArray ids = new JSONArray();
        for (String id : Arrays.asList("1", "2", "3")) {
            JSONObject obj = new JSONObject();
            obj.put("_id", id);
            ids.add(obj);
        }

        JSONObject condition = new JSONObject();
        condition.put("$or", ids);

        // 删除原有的数据
        log.info("删除的语句为：" + condition);
        DeleteResult deleteResult = mongoCollection.deleteMany(BsonDocument.parse(condition.toJSONString()));

        long count = deleteResult.getDeletedCount();
        log.info("---------- 删除 " + count + " 条数据成功");
    }

    /**
     * 演示几种查询时间日期的方法
     */
    public static void dateQuery() throws ParseException {
        BasicDBObject query;
        MongoCursor<Document> cursor;

        System.out.println("---------- String类型查询一");
        query = new BasicDBObject("birthday", new BasicDBObject("$regex", "1989-01-02 *"));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("---------- String类型查询二");
        // find({"insertTime":{$gte:"2019-02-01 00:00:00",$lte:"2019-02-11 23:59:59"}})
        query = new BasicDBObject("birthday", new BasicDBObject("$gte", "1989-01-02 11:32:33").append("$lte", "1995-01-02 11:22:33"));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("---------- Date类型，即IOSDate");
        Date startDate = format.parse("1993-01-02 11:22:33");
        Date endDate = format.parse("1998-01-02 11:22:33");

        BasicDBObject gte = new BasicDBObject("createTime", new BasicDBObject("$gte", startDate));
        BasicDBObject lt = new BasicDBObject("createTime", new BasicDBObject("$lt", endDate));

        query = new BasicDBObject("$and", Arrays.asList(gte, lt));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void listCollections() {
        ListCollectionsIterable<Document> documents = mongoDatabase.listCollections();
        documents.forEach((Consumer<? super Document>) d -> {
            System.out.println(d.getString("name"));
        });
    }

    public static void neExistsQuery() {
        BasicDBObject query;
        MongoCursor<Document> cursor;

        // 非查询
        query = new BasicDBObject("region", new BasicDBObject("$ne", "黄埔区"));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        // 查询有'email'字段的数据
        query = new BasicDBObject("email", new BasicDBObject("$exists", true));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void andQuery() {
        BasicDBObject query;
        MongoCursor<Document> cursor;

        System.out.println("---------- 单字段与查询");
        query = new BasicDBObject("age", new BasicDBObject("$gte", 25).append("$lte", 30));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("---------- 多字段与查询");
        query = new BasicDBObject();
        query.put("region", "黄埔区");
        query.put("age", 28);
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void orQuery() {
        BasicDBObject query;
        MongoCursor<Document> cursor;
        BasicDBList values;

        System.out.println("---------- 单字段或查询");
        values = new BasicDBList();
        values.add(new BasicDBObject("age", new BasicDBObject("$lte", 25)));
        values.add(new BasicDBObject("age", new BasicDBObject("$gt", 28)));
        query = new BasicDBObject("$or", values);
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("---------- 多字段或查询");
        values = new BasicDBList();
        values.add(new BasicDBObject("age", new BasicDBObject("$lte", 25)));
        values.add(new BasicDBObject("name", "张家10"));
        query = new BasicDBObject("$or", values);
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void inQuery() {
        BasicDBObject query = new BasicDBObject("_id", new BasicDBObject("$in", Arrays.asList("1", "2")));
        MongoCursor<Document> cursor = mongoCollection.find(query).iterator();
        System.out.println("in条件的使用");
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("not in条件的使用");
        query = new BasicDBObject("_id", new BasicDBObject("$not", new BasicDBObject("$in", Arrays.asList("1", "2"))));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void groupQuery() {
        List<BasicDBObject> pipeline = new ArrayList<BasicDBObject>();
        pipeline.add(new BasicDBObject("$group", new BasicDBObject("_id", "$region").append("count", new BasicDBObject("$sum", 1))));
        pipeline.add(new BasicDBObject("$sort", new BasicDBObject("count", -1)));
        AggregateIterable<Document> aggregate = mongoCollection.aggregate(pipeline);
        MongoCursor<Document> iterator = aggregate.iterator();

        while (iterator.hasNext()) {
            Document next = iterator.next();
            String area = next.getString("_id");
            int count = next.getInteger("count");
            System.out.println(area + ": " + count);
        }
    }

    public static void insertByte() {
        MongoUserInfo userInfo = new MongoUserInfo("张三", 1);
        MongoUser user = new MongoUser();
        user.setId(10000);
        user.setPwd("12345");
        user.setCreateTime(new Date());
        user.setInfo(userInfo);
        user.setUpdateTime("2019-03-07 21:06:28");
        user.setTitle(Arrays.asList("teacher", "professor", "doctor"));

        byte[] data = CommonUtil.serialize(user);

        Document doc = new Document("_id", "10000");
        doc.put("data", data);

        mongoCollection.insertOne(doc);
    }

    public static void readByte() {
        Document query = new Document("_id", "10000");
        Document doc = mongoCollection.find(query).first();

        Binary data = (Binary) doc.get("data");
        Object object = CommonUtil.deSerialize(data.getData());
        MongoUser user = (MongoUser) object;

        System.out.println(user);
    }

    public static void sortLimit() {
        BasicDBObject query;
        MongoCursor<Document> cursor;

        System.out.println("排序[1=asc，-1=desc]...");
        BsonDocument sort = new BsonDocument("_id", new BsonInt32(-1));
        query = new BasicDBObject();
        cursor = mongoCollection.find(query).sort(sort).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("---------- 分页查询");
//        cursor = mongoCollection.find(query).limit(1).iterator(); // 只取第1条数据
//        cursor = mongoCollection.find(query).skip(1).limit(1).iterator(); // 从1开始取1条数据，下标从10开始
        cursor = mongoCollection.find(query).sort(sort).skip(1).limit(5).iterator(); // DESC后取5条数据
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void likeQuery() {
        Pattern pattern = Pattern.compile("^.*家.*$", Pattern.CASE_INSENSITIVE);
        BasicDBObject query = new BasicDBObject("name", new BasicDBObject("$regex", pattern));
        MongoCursor<Document> cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void typeQuery() {
        BasicDBObject query;
        MongoCursor<Document> cursor;

        System.out.println("根据数据类型查询，字符类型...");
        query = new BasicDBObject("age", new BasicDBObject("$type", 2));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("根据数据类型查询，整型...");
        query = new BasicDBObject("age", new BasicDBObject("$type", 16));
        cursor = mongoCollection.find(query).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    /**
     * 查询数据的部分字段，1表示只返回本字段，0表示返回除此字段之外的字段，默认_id是会返回的
     */
    public static void projectQuery() {
        BasicDBObject query = new BasicDBObject();
        MongoCursor<Document> cursor;

        System.out.println("只要部分字段");
        cursor = mongoCollection.find(query).projection(BsonDocument.parse("{'name':1}")).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        System.out.println("只要部分字段之外的所有字段");
        cursor = mongoCollection.find(query).projection(BsonDocument.parse("{'name':0}")).iterator();
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    public static void indexOps() {
        ListIndexesIterable<Document> indexes = mongoCollection.listIndexes();
        System.out.println(indexes);
        indexes.forEach((Consumer<? super Document>) index -> {
            System.out.println(index);
        });

//        db.getCollection('user').ensureIndex({"name":1}, {background:true})
//        db.getCollection('user').ensureIndex({"name":1, "age":-1}, {background:true})

        // 索引的使用，创建索引[1=asc,-1=desc]，列出索引
        System.out.println("创建索引[1=asc,-1=desc]");
        IndexOptions indexOptions = new IndexOptions().background(true).name("name_index");
        mongoCollection.createIndex(new BasicDBObject("name", 1), indexOptions);

        mongoCollection.dropIndex("name_index");
    }

    public static void main(String[] args) {
        try {
            insertOne();
            bulkInsert();
            updateOne();
            bulkUpdate();
            deleteMany();
            listCollections();
            inQuery();
            neExistsQuery();
            andQuery();
            orQuery();
            groupQuery();
            dateQuery();
            insertByte();
            readByte();
            sortLimit();
            likeQuery();
            typeQuery();
            projectQuery();
            indexOps();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
