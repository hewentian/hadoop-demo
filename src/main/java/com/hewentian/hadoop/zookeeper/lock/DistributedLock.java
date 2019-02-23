package com.hewentian.hadoop.zookeeper.lock;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * <p>
 * <b>DistributedLock</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-22 23:35:31
 * @since JDK 1.8
 */
public class DistributedLock {
    private Logger log = Logger.getLogger(DistributedLock.class);

    private static final String GROUP_PATH = "/disLocks";
    private static final String SUB_PATH = GROUP_PATH + "/sub";
    private String LOG_PREFIX = Thread.currentThread().getName();

    private ZooKeeper zk;
    private Watcher watcher;
    private String selfPath;
    private String waitPath;

    private static volatile Boolean hasCreateParentPath = false;

    public DistributedLock(ZooKeeper zk) {
        this.zk = zk;
    }

    /**
     * 这个节点只会由一个线程创建成功
     *
     * @param threadName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public boolean createParentPath(String threadName) throws KeeperException, InterruptedException {
        synchronized (hasCreateParentPath) {
            if (zk.exists(GROUP_PATH, false) == null && hasCreateParentPath == false) {
                hasCreateParentPath = true;

                String data = "该节点由线程 " + threadName + " 创建";
                String res = zk.create(GROUP_PATH, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                log.info(LOG_PREFIX + " 节点创建成功，path: " + res + ", content: " + data);
            }
        }

        return true;
    }

    public boolean getLock() throws KeeperException, InterruptedException {
        selfPath = zk.create(SUB_PATH, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        log.info(LOG_PREFIX + " 创建锁路径: " + selfPath);

        if (checkMinPath()) {
            return true;
        }

        return false;
    }

    public void unlock() {
        try {
            if (zk.exists(selfPath, false) == null) {
                log.error(LOG_PREFIX + " 节点已经不存在: " + selfPath);
                return;
            }

            zk.delete(selfPath, -1);
            log.info(LOG_PREFIX + " 删除节点: " + selfPath);
        } catch (KeeperException e) {
            log.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        } finally {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public boolean checkMinPath() throws KeeperException, InterruptedException {
        List<String> subNodes = zk.getChildren(GROUP_PATH, false);
        Collections.sort(subNodes);
        int index = subNodes.indexOf(selfPath.substring(GROUP_PATH.length() + 1));
        switch (index) {
            case -1: {
                log.error(LOG_PREFIX + " 节点已经不存在: " + selfPath);
                return false;
            }
            case 0: {
                log.info(LOG_PREFIX + " 子节点队列中，我排在第一: " + selfPath);
                return true;
            }
            default: {
                waitPath = GROUP_PATH + "/" + subNodes.get(index - 1);
                log.info(LOG_PREFIX + " 获取子节点中，排在我前面的 " + waitPath);

                try {
                    zk.getData(waitPath, watcher, new Stat());
                    return false;
                } catch (KeeperException e) {
                    if (zk.exists(waitPath, false) == null) {
                        log.info(LOG_PREFIX + " 子节点中，排在我前面的 " + waitPath + " 已失踪，太好了！！！");
                        return checkMinPath();
                    } else {
                        throw e;
                    }
                }
            }
        }
    }

    public void setWatcher(Watcher watcher) {
        this.watcher = watcher;
    }

    public String getWaitPath() {
        return waitPath;
    }
}
