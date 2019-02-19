package com.hewentian.hadoop.utils;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 * <b>ZookeeperUtil</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-31 14:50:29
 * @since JDK 1.8
 */
public class ZookeeperUtil {
    private static Logger log = Logger.getLogger(ZookeeperUtil.class);

    private static String connectString = "hadoop-host-master:2181,hadoop-host-slave-1:2181,hadoop-host-slave-2:2181";

    private static ZooKeeper zooKeeper = null;

    static {
        try {
            getZookeeper();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static ZooKeeper getZookeeper() throws IOException {
        if (null == zooKeeper || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            // 设置一个watch监视zookeeper的变化
            zooKeeper = new ZooKeeper(connectString, 5000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    String path = event.getPath();
                    System.out.println("\n--------------------");
                    System.out.println("zookeeper." + event.getType().name() + ": " + path);
                    System.out.println("--------------------\n");
                }
            });

            if (zooKeeper.getState() != ZooKeeper.States.CONNECTING) {
                log.error("get connection error.");
                throw new IOException("get connection error.");
            }
        }

        return zooKeeper;
    }


    public static void close() {
        try {
            zooKeeper.close();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        Stat exists = zooKeeper.exists(path, watch);
        return null != exists;
    }

    public static void create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
        zooKeeper.create(path, data, acl, createMode);
    }

    public static byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return zooKeeper.getData(path, watch, stat);
    }

    public static Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return zooKeeper.setData(path, data, version);
    }

    /**
     * @param path
     * @param version -1 matches any node's versions
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void delete(String path, int version) throws KeeperException, InterruptedException {
        zooKeeper.delete(path, version);
    }

    /**
     * 获取孩子节点
     *
     * @param path
     * @param watch
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<String> getChildrenNode(String path, boolean watch) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(path, watch);
    }
}
