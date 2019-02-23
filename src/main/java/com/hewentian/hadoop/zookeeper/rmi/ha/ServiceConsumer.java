package com.hewentian.hadoop.zookeeper.rmi.ha;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.ConnectException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 * <p>
 * <b>ServiceConsumer</b> 是 服务消费者
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 15:45:06
 * @since JDK 1.8
 */
public class ServiceConsumer {
    private static final Logger log = Logger.getLogger(ServiceConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

    private volatile List<String> urlList = new ArrayList<>();

    public ServiceConsumer() {
        ZooKeeper zk = connectServer();
        if (zk != null) {
            watchNode(zk);
        }
    }

    public <T extends Remote> T lookup() {
        T service = null;
        int size = urlList.size();

        if (size > 0) {
            String url;
            if (size == 1) {
                url = urlList.get(0);
                log.info(String.format("using only url: %s", url));
            } else {
                url = urlList.get(ThreadLocalRandom.current().nextInt(size)); // 随机获取一个元素
                log.info(String.format("using random url: %s", url));
            }

            service = lookupService(url);
        }

        return service;
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper(Constant.ZK_CONNECTION_STRING, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });

            latch.await();
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        return zk;
    }

    // 观察 /registry 节点下所有子节点是否有变化
    private void watchNode(final ZooKeeper zk) {
        try {
            List<String> nodeList = zk.getChildren(Constant.ZK_REGISTRY_PATH, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        watchNode(zk);
                    }
                }
            });

            List<String> urlListTmp = new ArrayList<>();
            for (String node : nodeList) {
                byte[] data = zk.getData(Constant.ZK_REGISTRY_PATH + "/" + node, false, null);
                urlListTmp.add(new String(data));
            }

            log.debug(String.format("node data: %s", urlListTmp));
            urlList = urlListTmp;
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T lookupService(String url) {
        T remote = null;

        try {
            remote = (T) Naming.lookup(url);
        } catch (NotBoundException | MalformedURLException | RemoteException e) {
            if (e instanceof ConnectException) {
                log.error(String.format("ConnectException -> url: %s", url));

                // 若连接中断，则使用 urlList 中第一个 RMI 地址来查找
                if (urlList.size() != 0) {
                    url = urlList.get(0);
                    return lookupService(url);
                }
            }

            log.error(e.getMessage(), e);
        }

        return remote;
    }
}
