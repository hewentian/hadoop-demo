package com.hewentian.hadoop.hive;

import com.hewentian.hadoop.utils.HiveUtil;
import org.apache.log4j.Logger;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * <p>
 * <b>HiveDemo</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-01-13 12:38:44
 * @since JDK 1.8
 */
public class HiveDemo {
    private static Logger log = Logger.getLogger(HiveDemo.class);

    public static void count() throws SQLException {
        String sql = "select count(1) from t_user";
        ResultSet rs = HiveUtil.executeQuery(sql, null);

        if (rs.next()) {
            long count = rs.getLong(1);
            log.info("总数为： " + count); // 总数为： 3
        }
    }

    public static void query() throws SQLException {
        String sql = "select * from t_user";
        ResultSet rs = HiveUtil.executeQuery(sql, null);
        while (rs.next()) {
            StringBuilder sb = new StringBuilder();
            sb.append(rs.getInt(1)).append("\t");
            sb.append(rs.getString(2)).append("\t");
            sb.append(rs.getInt(3)).append("\t");
            sb.append(rs.getString(4)).append("\t");
            sb.append(rs.getString(5)).append("\t");
            sb.append(rs.getString(6));

            log.info(sb.toString());
//            1	Tim Ho	23	M	1989-05-01	Higher Education Mega Center South, Guangzhou city, Guangdong Province
//            2	scott	25	M	1977-10-21	USA
//            3	tiger	21	F	1977-08-12	UK
        }
    }

    public static void main(String[] args) {
        try {
//            count();
            query();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
