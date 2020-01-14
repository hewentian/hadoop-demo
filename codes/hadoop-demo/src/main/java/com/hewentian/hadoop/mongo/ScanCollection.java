package com.hewentian.hadoop.mongo;

import com.hewentian.hadoop.utils.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;

/**
 * <p>
 * <b>ScanCollection</b> 是 单线程遍历集合，用于集合的数据量在百万级别以下。
 * <br>
 * 遍历集合，不要使用skip、limit来分页操作。如果集合很大，则会很慢。这里介绍一种比较快的方法
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-11-12 10:18:22
 * @since JDK 1.8
 */
public class ScanCollection {
    public static void main(String[] args) {
        MongoDatabase mongoDatabase = MongoUtil.getMongoDatabase();
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("user");

        Long totalCount = mongoCollection.count();
        System.out.println("总记录数： " + totalCount);

        if (totalCount.intValue() == 0) {
            System.out.println("no data to handle.");
            return;
        }

        String threadName = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();

        int pageSize = 100;
        int pageCount = totalCount.intValue() / pageSize;
        int modCount = totalCount.intValue() % pageSize;

        // 开始的collection记录的_id，根据_id分段来查询，每一段都获取之后，会将最后一个的_id赋给它
        BsonDocument sort = new BsonDocument("_id", new BsonInt32(1));
        String startId = mongoCollection.find().sort(sort).first().getString("_id");

        // 当然，我们也可以从指定的位置开始遍历。这时，就要修改上面的
//        startId = "2";
//        totalCount = mongoCollection.count(new BasicDBObject("_id", new BasicDBObject("$gte", startId)));

        for (int i = 0; i < pageCount; i++) {
            if (i == pageCount - 1) {
                pageSize += modCount;
            }

            MongoCursor<Document> dbCursor;
            if (i == 0) {
                // 第一次才会进入这里
                dbCursor = mongoCollection.find(new BasicDBObject("_id", new BasicDBObject("$gte", startId)))
                        .sort(sort).limit(pageSize)
                        .iterator();
            } else {
                dbCursor = mongoCollection.find(new BasicDBObject("_id", new BasicDBObject("$gt", startId)))
                        .sort(sort).limit(pageSize)
                        .iterator();
            }

            System.out.println(threadName + ": currPage: " + i + " / " + pageCount);

            while (dbCursor.hasNext()) {
                Document doc = dbCursor.next();

                String _id = doc.getString("_id");
                String name = doc.getString("name");
                Integer age = doc.getInteger("age");

                if (!dbCursor.hasNext()) {
                    startId = _id;
                }

                System.out.println(name + ", " + age);
            }
        }

        System.out.println(threadName + ": end, cost: " + (System.currentTimeMillis() - startTime));
    }
}
