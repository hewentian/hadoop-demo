package com.hewentian.hadoop.zookeeper.rmi.ha;

import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * <p>
 * <b>ServiceProvider</b> 是 服务提供者，用于发布RMI服务。
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 15:20:48
 * @since JDK 1.8
 */
public class ServiceProvider {
    private static final Logger log = Logger.getLogger(ServiceProvider.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public void publish(Remote remote, String host, int port) {
        String url = publishService(remote, host, port);
        if (url != null) {
            ZooKeeper zk = connectServer();
            if (zk != null) {
                checkParentPath(zk);
                createNode(zk, url);
            }
        }
    }

    private String publishService(Remote remote, String host, int port) {
        String url = null;

        try {
            url = String.format("rmi://%s:%d/%s", host, port, remote.getClass().getName());
            LocateRegistry.createRegistry(port);
            Naming.rebind(url, remote);

            log.debug(String.format("publish rmi service (url: %s)", url));
        } catch (RemoteException | MalformedURLException e) {
            log.error(e.getMessage(), e);
        }

        return url;
    }

    private ZooKeeper connectServer() {
        ZooKeeper zk = null;

        try {
            zk = new ZooKeeper(Constant.ZK_CONNECTION_STRING, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown(); // 唤醒当前正在执行的线程
                    }
                }
            });

            latch.await(); // 使当前线程处于等待状态
        } catch (IOException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }

        return zk;
    }

    /**
     * 检查父目录是否存在，如果不存在则创建，我们也可以使用ZooKeeper的客户端工具创建：
     * create /registry null
     *
     * @param zk
     */
    public void checkParentPath(ZooKeeper zk) {
        try {
            if (zk.exists(Constant.ZK_REGISTRY_PATH, false) == null) {
                zk.create(Constant.ZK_REGISTRY_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.debug(String.format("create zookeeper node: %s", Constant.ZK_REGISTRY_PATH));
            }
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void createNode(ZooKeeper zk, String url) {
        try {
            byte[] data = url.getBytes();
            String path = zk.create(Constant.ZK_PROVIDER_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            log.debug(String.format("create zookeeper node (%s => %s)", path, url));
        } catch (KeeperException | InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }
}
