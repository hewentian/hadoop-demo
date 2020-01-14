package com.hewentian.hadoop.zookeeper.lock;

import com.hewentian.hadoop.zookeeper.rmi.ha.Constant;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * <p>
 * <b>LockService</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-22 15:37:26
 * @since JDK 1.8
 */
public class LockService {
    private static final Logger log = Logger.getLogger(LockService.class);
    private AbstractZookeeper az = new AbstractZookeeper();

    public void doService(DoTemplate doTemplate) {
        try {
            ZooKeeper zk = az.connect(Constant.ZK_CONNECTION_STRING, Constant.ZK_SESSION_TIMEOUT);
            DistributedLock distributedLock = new DistributedLock(zk);
            LockWatcher lockWatcher = new LockWatcher(distributedLock, doTemplate);
            distributedLock.setWatcher(lockWatcher);
            distributedLock.createParentPath(Thread.currentThread().getName());

            boolean getLock = distributedLock.getLock();
            if (getLock) { // 如果第一次即获取到锁，则马上执行。否则，由LockWatcher中监听执行
                lockWatcher.doSomething();
                distributedLock.unlock();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } catch (KeeperException e) {
            log.error(e.getMessage(), e);
        }
    }
}
