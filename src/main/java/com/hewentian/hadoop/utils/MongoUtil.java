package com.hewentian.hadoop.utils;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;

import java.util.Arrays;

/**
 * <p>
 * <b>MongoUtil</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-03-05 19:52:02
 * @since JDK 1.8
 */
public class MongoUtil {
    private MongoUtil() {
    }

    public static MongoDatabase getMongoDatabase() {
        String mongoDbHost = Config.get("mongodb.host", null);
        String mongoDbPort = Config.get("mongodb.port", null);
        String mongoDbUserName = Config.get("mongodb.username", null);
        String mongoDbPassword = Config.get("mongodb.password", null);
        String mongoDbAauthenticationDatabase = Config.get("mongodb.authentication-database", null);
        String mongoDbDatabase = Config.get("mongodb.database", null);

        MongoCredential credential = MongoCredential.createCredential(mongoDbUserName, mongoDbAauthenticationDatabase,
                mongoDbPassword.toCharArray());
        MongoClient mongoClient = new MongoClient(new ServerAddress(mongoDbHost, Integer.parseInt(mongoDbPort)),
                Arrays.asList(credential));
        MongoDatabase db = mongoClient.getDatabase(mongoDbDatabase);

        return db;
    }
}
