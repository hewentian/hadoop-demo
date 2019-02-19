package com.hewentian.hadoop.zookeeper;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import com.hewentian.hadoop.utils.ZookeeperUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

/**
 * <p>
 * <b>ZookeeperDemo</b> 是 zookeeper操作例子
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-31 15:50:39
 * @since JDK 1.8
 */
public class ZookeeperDemo {
    private static String persistentNode = "/zk_node_persistent";
    private static String ephemeralNode = "/zk_node_ephemeral";

    public static void testGetChildrenNode() throws KeeperException, InterruptedException {
        String parentPath = "/";
        List<String> childrenNode = ZookeeperUtil.getChildrenNode(parentPath, true);
        System.out.println("childrenNode size: " + childrenNode.size());

        for (String s : childrenNode) {
            System.out.println(parentPath + "/" + s);
        }
    }

    public static void testCreate() throws KeeperException, InterruptedException {
        // 新增节点 CreateMode.PERSISTENT
        if (ZookeeperUtil.exists(persistentNode, true) == false) {
            // CreateMode.PERSISTENT这种节点当失去与zookeeper的联系时也不会消失
            ZookeeperUtil.create(persistentNode, "I am persistent".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 新增节点 CreateMode.EPHEMERAL
        if (ZookeeperUtil.exists(ephemeralNode, true) == false) {
            // CreateMode.EPHEMERAL这种节点当失去与zookeeper的联系时会消失
            ZookeeperUtil.create(ephemeralNode, "I am ephemeral".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }
    }

    public static void testExists() throws KeeperException, InterruptedException {
        System.out.println(ZookeeperUtil.exists(persistentNode, true));
        System.out.println(ZookeeperUtil.exists(ephemeralNode, true));
    }

    public static void testGetData() throws KeeperException, InterruptedException {
        byte[] data = ZookeeperUtil.getData(persistentNode, true, null);
        System.out.println("nodeValue: " + new String(data));
    }

    public static void testSetData() throws KeeperException, InterruptedException {
        // 更新前
        byte[] data = ZookeeperUtil.getData(persistentNode, true, null);
        System.out.println("nodeValue: " + new String(data));

        // 更新node中的数据，并再次将其读出
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String newValue = "I am persistent " + df.format(new Date());
        Stat stat = ZookeeperUtil.setData(persistentNode, newValue.getBytes(), -1);
        System.out.println(stat.getVersion());

        // 更新后
        data = ZookeeperUtil.getData(persistentNode, true, null);
        System.out.println("nodeValue: " + new String(data));
    }

    public static void testDelete() throws KeeperException, InterruptedException {
        ZookeeperUtil.delete(persistentNode, -1);
    }

    public static void main(String[] args) {
        try {
            testGetChildrenNode();
            testCreate();
            testExists();
            testGetData();
            testSetData();
            testDelete();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            ZookeeperUtil.close();
        }
    }
}