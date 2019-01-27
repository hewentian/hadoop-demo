package com.hewentian.hadoop.utils;

import com.hewentian.hadoop.entity.User;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * <b>HbaseUtil</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-24 17:05:02
 * @since JDK 1.8
 */
public class HbaseUtil {
    private static Logger log = Logger.getLogger(HbaseUtil.class);

    private static Connection conn = null;

    private HbaseUtil() {
    }

    public static Connection getConnection() throws IOException {
        if (null == conn || conn.isClosed()) {
            Configuration conf = HBaseConfiguration.create();

            // 集群配置时，只需指出zookeeper地址即可，zookeeper自已会找到hbase.master
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("hbase.zookeeper.quorum", "hadoop-host-master,hadoop-host-slave-1,hadoop-host-slave-2");

            // 非集群配置时，例如单机模式下，就需指定hbase.master
            //  conf.set("hbase.master", "127.0.0.1:60000");

            try {
                conn = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                throw e;
            }
        }

        return conn;
    }

    public static void close() {
        try {
            conn.close();
            conn = null;
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }


    /**
     * 创建表
     *
     * @param tableName_ 表名
     * @param cols       列族数组
     */
    public static void createTable(String tableName_, String[] cols) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Admin admin = getConnection().getAdmin();

        if (admin.tableExists(tableName)) {
            log.info("table " + tableName_ + " exists.");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            admin.createTable(hTableDescriptor);

            log.info("create table " + tableName_ + " successfully.");
        }
    }

    public static void insertData(String tableName_, User user) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Put put = new Put(user.getId().getBytes());

        // 列族名，列名，列值
        put.addColumn("info".getBytes(), "username".getBytes(), user.getUsername().getBytes());
        put.addColumn("info".getBytes(), "gender".getBytes(), user.getGender().getBytes());
        put.addColumn("info".getBytes(), "birthday".getBytes(), user.getBirthday().getBytes());

        put.addColumn("contact".getBytes(), "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn("contact".getBytes(), "email".getBytes(), user.getEmail().getBytes());
        put.addColumn("contact".getBytes(), "address".getBytes(), user.getAddress().getBytes());

        Table table = getConnection().getTable(tableName);
        table.put(put);
    }

    public static List<User> scanTable(String tableName_) throws IOException {
        List<User> users = new ArrayList<User>();

        TableName tableName = TableName.valueOf(tableName_);
        Table table = getConnection().getTable(tableName);
        ResultScanner results = table.getScanner(new Scan());

        for (Result result : results) {
//            显示未解析的数据
//            log.info(result);
            User user = new User();

            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

                user.setId(row); // 其实设置一次就行

                if ("username".equals(colName)) {
                    user.setUsername(value);
                } else if ("gender".equals(colName)) {
                    user.setGender(value);
                } else if ("birthday".equals(colName)) {
                    user.setBirthday(value);
                } else if ("phone".equals(colName)) {
                    user.setPhone(value);
                } else if ("email".equals(colName)) {
                    user.setEmail(value);
                } else if ("address".equals(colName)) {
                    user.setAddress(value);
                }
            }

            users.add(user);
        }

        return users;
    }

    public static User getDataByRowKey(String tableName_, String rowKey) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Table table = getConnection().getTable(tableName);

        // 先判断数据是否存在
        Get get = new Get(rowKey.getBytes());
        get.setCheckExistenceOnly(true);

        Result result = table.get(get);
        if (!result.getExists()) {
            return null;
        }

        // 数据存在了，再获取
        get = new Get(rowKey.getBytes());
        get.setCheckExistenceOnly(false);
        result = table.get(get);

        User user = new User();
        user.setId(rowKey);

        for (Cell cell : result.rawCells()) {
            String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());

            if ("username".equals(colName)) {
                user.setUsername(value);
            } else if ("gender".equals(colName)) {
                user.setGender(value);
            } else if ("birthday".equals(colName)) {
                user.setBirthday(value);
            } else if ("phone".equals(colName)) {
                user.setPhone(value);
            } else if ("email".equals(colName)) {
                user.setEmail(value);
            } else if ("address".equals(colName)) {
                user.setAddress(value);
            }
        }

        return user;
    }

    public static String getCellData(String tableName_, String rowKey, String family, String qualifier) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Table table = getConnection().getTable(tableName);

        Get get = new Get(rowKey.getBytes());
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        get.setCheckExistenceOnly(true);

        Result result = table.get(get);
        if (!result.getExists()) {
            throw new IOException("the data you search not exists");
        }

        get.setCheckExistenceOnly(false);
        result = table.get(get);

        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        String res = Bytes.toString(value);

        return res;
    }

    /**
     * 删除数据，可以删除一行，或者一个列族，或者一列
     *
     * @param tableName_
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void deleteByRowKey(String tableName_, String rowKey, String family, String qualifier) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Table table = getConnection().getTable(tableName);

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        if (StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        } else if (StringUtils.isNotBlank(family) && StringUtils.isBlank(qualifier)) {
            delete.addFamily(Bytes.toBytes(family));
        }

        table.delete(delete);
    }

    public static void deleteTable(String tableName_) throws IOException {
        TableName tableName = TableName.valueOf(tableName_);
        Admin admin = getConnection().getAdmin();

        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }
}
