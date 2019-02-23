package com.hewentian.hadoop.zookeeper.lock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * <p>
 * <b>AbstractZookeeper</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-22 15:30:06
 * @since JDK 1.8
 */
public class AbstractZookeeper implements Watcher {
    private Logger log = Logger.getLogger(AbstractZookeeper.class);
    protected ZooKeeper zooKeeper;
    protected CountDownLatch latch = new CountDownLatch(1);

    public ZooKeeper connect(String hosts, int sessionTimeout) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(hosts, sessionTimeout, this);
        latch.await();

        log.info("connect zookeeper success");
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            latch.countDown();
        }
    }
}
