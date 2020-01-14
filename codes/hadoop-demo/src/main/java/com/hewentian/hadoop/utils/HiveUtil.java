package com.hewentian.hadoop.utils;

import org.apache.log4j.Logger;

import java.sql.*;

/**
 * <p>
 * <b>HiveUtil</b> 是 hive工具类
 * 必须运行hiveserver2，方法如下：
 * $ cd /home/hadoop/apache-hive-1.2.2-bin/bin/
 * $ ./hiveserver2
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-13 12:41:43
 * @since JDK 1.8
 */
public class HiveUtil {
    private static Logger log = Logger.getLogger(HiveUtil.class);

    private HiveUtil() {
    }

    private static String url = "jdbc:hive2://hadoop-host-master:10000/tim";
    private static String username = "hadoop";
    private static String password = "hadoop";
    private static String driverClassName = "org.apache.hive.jdbc.HiveDriver";

    private static Connection conn = null;

    static {
        try {
            Class.forName(driverClassName);
        } catch (Exception e) {
            log.error(e);
        }
    }

    public static Connection getStaticConnection() {
        try {
            if (null == conn || conn.isClosed()) {
                conn = getConnection();
            }

            if (null == conn || conn.isClosed()) {
                log.error("can't get conn");
                return null;
            }
        } catch (Exception e) {
            log.error(e);
        }

        return conn;
    }


    public static Connection getConnection() {
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, username, password); // 获取连接
        } catch (Exception e) {
            log.error(e);
        }

        return conn;
    }

    public static ResultSet executeQuery(String sql, Object[] params) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = getStaticConnection();
            ps = conn.prepareStatement(sql);
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            rs = ps.executeQuery();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {

        }
        return rs;
    }


    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        try {
            if (null != rs && !rs.isClosed()) {
                rs.close();
                rs = null;
            }
        } catch (Exception e) {
            log.error(e);
        }

        try {
            if (null != stmt && !stmt.isClosed()) {
                stmt.close();
                stmt = null;
            }
        } catch (Exception e) {
            log.error(e);
        }

        try {
            if (null != conn && !conn.isClosed()) {
                conn.close();
                conn = null;
            }
        } catch (SQLException e) {
            log.error(e);
        }
    }
}
