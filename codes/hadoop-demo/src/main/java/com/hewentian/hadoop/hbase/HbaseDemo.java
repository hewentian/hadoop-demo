package com.hewentian.hadoop.hbase;

import com.hewentian.hadoop.entity.User;
import com.hewentian.hadoop.utils.HbaseUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * <p>
 * <b>HbaseDemo</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-25 14:14:41
 * @since JDK 1.8
 */
public class HbaseDemo {
    private static Logger log = Logger.getLogger(HbaseDemo.class);
    private static String tableName_ = "t_user";

    public static void testCreateTable() throws IOException {
        HbaseUtil.createTable(tableName_, new String[]{"info", "contact"});
    }

    public static void testInsertData() throws IOException {
        User user = new User();
        user.setId("001");
        user.setUsername("张三");
        user.setGender("女");
        user.setBirthday("1990-01-20");
        user.setPhone("12345");
        user.setEmail("zhansan@qq.com");
        user.setAddress("北京市丰台区");
        HbaseUtil.insertData(tableName_, user);

        user = new User();
        user.setId("002");
        user.setUsername("李四");
        user.setGender("男");
        user.setBirthday("1991-02-21");
        user.setPhone("67890");
        user.setEmail("lisi@qq.com");
        user.setAddress("广州市天河区");
        HbaseUtil.insertData(tableName_, user);
    }

    public static void testScanTable() throws IOException {
        // 遍历所有
        List<User> users = HbaseUtil.scanTable(tableName_, null, null, null, null);
        log.info("user.size: " + users.size());

        for (User user : users) {
            log.info(user);
        }

        // output:
        // User{id='001', username='张三', gender='女', phone='12345', birthday='1990-01-20', email='zhansan@qq.com', address='北京市丰台区'}
        // User{id='002', username='李四', gender='男', phone='67890', birthday='1991-02-21', email='lisi@qq.com', address='广州市天河区'}

        // 遍历指定的列
        users = HbaseUtil.scanTable(tableName_, null, null, "info", "username");
        log.info("user.size: " + users.size());

        for (User user : users) {
            log.info(user);
        }

        // output:
        // User{id='001', username='张三', gender='null', phone='null', birthday='null', email='null', address='null'}
        // User{id='002', username='李四', gender='null', phone='null', birthday='null', email='null', address='null'}

        // 遍历指定的列族
        users = HbaseUtil.scanTable(tableName_, null, null, "info", null);
        log.info("user.size: " + users.size());

        for (User user : users) {
            log.info(user);
        }

        // output:
        // User{id='001', username='张三', gender='女', phone='null', birthday='1990-01-20', email='null', address='null'}
        // User{id='002', username='李四', gender='男', phone='null', birthday='1991-02-21', email='null', address='null'}
    }

    public static void testGetDataByRowKey() throws IOException {
        User user = HbaseUtil.getDataByRowKey(tableName_, "001");
        log.info(user);

        // output:
        // User{id='001', username='张三', gender='女', phone='12345', birthday='1990-01-20', email='zhansan@qq.com', address='北京市丰台区'}
    }

    public static void testGetCellData() throws IOException {
        String value = HbaseUtil.getCellData(tableName_, "001", "contact", "email", null);
        log.info(value);

        // output:
        // zhansan@qq.com
    }

    public static void testDeleteByRowKey() throws IOException {
        // 删除前
        User user = null;
        user = HbaseUtil.getDataByRowKey(tableName_, "001");
        log.info(user);

        // output:
        // User{id='001', username='张三', gender='女', phone='12345', birthday='1990-01-20', email='zhansan@qq.com', address='北京市丰台区'}

        HbaseUtil.deleteByRowKey(tableName_, "001", "contact", "email");
        user = HbaseUtil.getDataByRowKey(tableName_, "001");
        log.info(user);

        // output:
        // User{id='001', username='张三', gender='女', phone='12345', birthday='1990-01-20', email='null', address='北京市丰台区'}

        HbaseUtil.deleteByRowKey(tableName_, "001", "contact", null);
        user = HbaseUtil.getDataByRowKey(tableName_, "001");
        log.info(user);

        // output:
        // User{id='001', username='张三', gender='女', phone='null', birthday='1990-01-20', email='null', address='null'}

        HbaseUtil.deleteByRowKey(tableName_, "001", null, null);
        user = HbaseUtil.getDataByRowKey(tableName_, "001");
        log.info(user);

        // output:
        // null
    }

    public static void testDeleteTable() throws IOException {
        HbaseUtil.deleteTable(tableName_);
    }

    public static void testDescTable() throws IOException {
        HbaseUtil.descTable(tableName_);
    }

    public static void main(String[] args) {
        try {
            testCreateTable();
            testInsertData();
            testScanTable();
            testGetDataByRowKey();
            testGetCellData();
            testDeleteByRowKey();
            testDeleteTable();
            testDescTable();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } finally {
            HbaseUtil.close();
        }
    }
}
