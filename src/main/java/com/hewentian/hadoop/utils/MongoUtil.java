package com.hewentian.hadoop.utils;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    /**
     * 连接到单机
     *
     * @return
     */
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

    /**
     * 连接到副本集
     *
     * @return
     */
    public static MongoDatabase getMongoDatabaseReplicaSet() {
        String mongoDbHost = Config.get("mongodb.host", "192.168.1.111,192.168.1.112,192.168.1.113");
        String mongoDbPort = Config.get("mongodb.port", null);
        String mongoDbUserName = Config.get("mongodb.username", null);
        String mongoDbPassword = Config.get("mongodb.password", null);
        String mongoDbAauthenticationDatabase = Config.get("mongodb.authentication-database", null);
        String mongoDbDatabase = Config.get("mongodb.database", null);
        String mongoDbReplicaSet = Config.get("mongodb.replica-set", null);

        MongoCredential credential = MongoCredential.createCredential(mongoDbUserName, mongoDbAauthenticationDatabase,
                mongoDbPassword.toCharArray());

        MongoClientOptions options = MongoClientOptions.builder().requiredReplicaSetName(mongoDbReplicaSet)
                .writeConcern(WriteConcern.W1).readPreference(ReadPreference.secondaryPreferred()).build();

        List<ServerAddress> serverAddressList = new ArrayList<>();
        for (String host : mongoDbHost.split(",")) {
            serverAddressList.add(new ServerAddress(host, Integer.parseInt(mongoDbPort)));
        }

        MongoClient mongoClient = new MongoClient(serverAddressList, Arrays.asList(credential), options);
        MongoDatabase db = mongoClient.getDatabase(mongoDbDatabase);

        return db;
    }
}
