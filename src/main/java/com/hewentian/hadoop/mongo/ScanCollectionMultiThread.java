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
 * <b>ScanCollectionMultiThread</b> 是 多线程遍历集合，用于集合的数据量在千万级别以上
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-11-12 10:23:21
 * @since JDK 1.8
 */
public class ScanCollectionMultiThread {
    public static void main(String[] args) {
        MongoDatabase mongoDatabase = MongoUtil.getMongoDatabase();
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("user");

        Long totalCount = mongoCollection.count();
        System.out.println("总记录数： " + totalCount);

        if (totalCount.intValue() == 0) {
            System.out.println("no data to handle.");
            return;
        }

        int threadNum = 8; // 线程数
        int batchSize = totalCount.intValue() / threadNum; // 每个线程处理的数据量
        int batchSizeMod = totalCount.intValue() % threadNum;

        for (int i = 0; i < threadNum; i++) {
            int skip = i * batchSize;
            int batch = batchSize;

            if (i == threadNum - 1) {
                batch += batchSizeMod;
            }

            Thread t = new Thread(new MyHandle(skip, batch), "thread-" + i);
            t.start();
        }

        System.out.println("all thread started");
    }

    static class MyHandle implements Runnable {
        MongoDatabase mongoDatabase = MongoUtil.getMongoDatabase();
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("user");
        BsonDocument sort = new BsonDocument("_id", new BsonInt32(1));

        int skip;
        int batchSize;

        public MyHandle(int skip, int batchSize) {
            this.skip = skip;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            String threadName = Thread.currentThread().getName();
            long startTime = System.currentTimeMillis();

            System.out.println(threadName + ": [" + skip + ", " + batchSize + "]");

            int pageSize = 100;
            int pageCount = batchSize / pageSize;
            int modCount = batchSize % pageSize;

            // 开始的collection记录的_id，根据_id分段来查询，每一段都获取之后，会将最后一个的_id赋给它
            String startId = mongoCollection.find().sort(sort).skip(skip).first().getString("_id");

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
}
