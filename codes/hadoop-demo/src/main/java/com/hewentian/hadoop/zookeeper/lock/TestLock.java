package com.hewentian.hadoop.zookeeper.lock;

import org.apache.log4j.Logger;

import java.util.concurrent.CountDownLatch;

/**
 * <p>
 * <b>TestLock</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-22 15:10:18
 * @since JDK 1.8
 */
public class TestLock {
    private static final Logger log = Logger.getLogger(TestLock.class);
    private static final int THREAD_NUM = 10;
    public static CountDownLatch latch = new CountDownLatch(THREAD_NUM);

    public static void main(String[] args) {
        for (int i = 0; i < THREAD_NUM; i++) {
            final int threadId = i;
            new Thread() {
                @Override
                public void run() {
                    try {
                        new LockService().doService(new DoTemplate() {
                            @Override
                            public void doInvoke() {
                                log.info("我要修改一个文件：" + threadId);
                            }
                        });
                    } catch (Exception e) {
                        log.error("第 " + threadId + " 个线程抛出异常", e);
                    }
                }
            }.start();
        }

        try {
            latch.await();
            log.info("all threads run successful.");
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
