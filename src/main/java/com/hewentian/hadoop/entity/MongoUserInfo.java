package com.hewentian.hadoop.entity;

import java.io.Serializable;

/**
 * <p>
 * <b>T</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-03-06 09:21:13
 * @since JDK 1.8
 */
public class MongoUserInfo implements Serializable {
    private static final long serialVersionUID = -6890227146194038339L;

    private String name;

    /**
     * 1.男；2.女
     */
    private int sex;

    public MongoUserInfo() {
        super();
    }

    public MongoUserInfo(String name, int sex) {
        super();
        this.name = name;
        this.sex = sex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    @Override
    public String toString() {
        return "MongoUserInfo{" +
                "name='" + name + '\'' +
                ", sex=" + sex +
                '}';
    }
}
