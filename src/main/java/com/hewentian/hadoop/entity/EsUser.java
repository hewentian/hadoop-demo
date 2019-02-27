package com.hewentian.hadoop.entity;

import io.searchbox.annotations.JestId;

import java.util.Arrays;
import java.util.Date;

/**
 * <p>
 * <b>EsUser</b> 是 测试es用的bean
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-09-18 4:25:33 PM
 * @since JDK 1.8
 */
public class EsUser implements Cloneable {
    @JestId
    private Long id;
    private String name;
    private Integer age;
    private String[] tags;
    private Date birthday;

    public EsUser() {
        super();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String[] getTags() {
        return tags;
    }

    public void setTags(String[] tags) {
        this.tags = tags;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    @Override
    public String toString() {
        return "EsUser [id=" + id + ", name=" + name + ", age=" + age + ", tags=" + Arrays.toString(tags)
                + ", birthday=" + birthday + "]";
    }
}
