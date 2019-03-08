package com.hewentian.hadoop.entity;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * <b>T</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-03-06 09:22:14
 * @since JDK 1.8
 */
public class MongoUser implements Serializable {
    private static final long serialVersionUID = 1L;

    private int id;
    private String pwd;
    private MongoUserInfo info;
    private List<String> title;
    private String updateTime; // 两种时间格式，它们的查询方式不同
    private Date createTime;

    public MongoUser() {
        super();
    }

    public MongoUser(int id, String pwd, MongoUserInfo info) {
        super();
        this.id = id;
        this.pwd = pwd;
        this.info = info;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPwd() {
        return pwd;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    public MongoUserInfo getInfo() {
        return info;
    }

    public void setInfo(MongoUserInfo info) {
        this.info = info;
    }

    public List<String> getTitle() {
        return title;
    }

    public void setTitle(List<String> title) {
        this.title = title;
    }

    public String getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(String updateTime) {
        this.updateTime = updateTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "MongoUser{" +
                "id=" + id +
                ", pwd='" + pwd + '\'' +
                ", info=" + info +
                ", title=" + title +
                ", updateTime='" + updateTime + '\'' +
                ", createTime=" + createTime +
                '}';
    }
}
