package com.hewentian.hadoop.redis;

import com.hewentian.hadoop.utils.RedisUtil;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * <p>
 * <b>RedisDemo</b> 是 redis 测试
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2018-09-15 11:12:30 AM
 * @since JDK 1.8
 */
public class RedisDemo {
    public static void main(String[] args) {
        test();
        testPool();
    }

    private static void test() {
        Jedis jedis = RedisUtil.getJedis();

        // 选择第0号库
        jedis.select(0);

        // 简单的key-value存储
        jedis.set("name", "Tim");
        System.out.println(jedis.get("name")); // Tim

        jedis.append("name", " Ho");
        jedis.append("age", "23");
        System.out.println(jedis.get("name")); // Tim Ho
        System.out.println(jedis.get("age")); // 23

        System.out.println(jedis.exists("name")); // true
        System.out.println(jedis.exists("sex")); // false

        // mset是设置多个key-value值，参数(key1, value1, key2, value2,..., keyn, valuen)
        // mget是获取多个key所对应的value，参数(key1, key2, key3, ..., keyn)返回的是个list
        jedis.mset("name1", "tim1", "name2", "tim2", "name3", "tim3");
        System.out.println(jedis.mget("name1", "name2", "name3")); // [tim1, tim2, tim3]

        // map
        Map<String, String> user = new HashMap<String, String>();
        user.put("name", "scott");
        user.put("password", "tiger");

        // map 存入redis
        jedis.hmset("user", user);
        jedis.hset("user", "age", "23");
        // mapkey个数
        System.out.println(String.format("len: %d", jedis.hlen("user"))); // len: 3
        // map中的所有键值
        System.out.println(String.format("keys: %s", jedis.hkeys("user"))); // keys: [name, password, age]
        // map中的所有value
        System.out.println(String.format("values: %s", jedis.hvals("user"))); // values: [tiger, scott, 23]
        // 取出map中的name字段
        List<String> userValues = jedis.hmget("user", "name", "password");
        System.out.println(userValues); // [scott, tiger]
        // 删除map中的某一个键值password
        jedis.hdel("user", "password");
        System.out.println(jedis.hmget("user", "name", "password")); // [scott, null]

        // list
        jedis.del("listDemo");
        System.out.println(jedis.lrange("listDemo", 0, -1)); // []
        jedis.lpush("listDemo", "A");
        jedis.lpush("listDemo", "B");
        jedis.lpush("listDemo", "C");
        System.out.println(jedis.lrange("listDemo", 0, -1)); // [C, B, A]
        System.out.println(jedis.lrange("listDemo", 0, 1)); // [C, B]

        // set
        jedis.sadd("sname", "h");
        jedis.sadd("sname", "w");
        jedis.sadd("sname", "t");
        jedis.sadd("sname", "t");
        System.out.println(String.format("set num: %d", jedis.scard("sname"))); // set num: 3
        System.out.println(String.format("all members: %s", jedis.smembers("sname"))); // all members: [h, w, t]
        System.out.println(String.format("is member: %B", jedis.sismember("sname", "h"))); // is member: TRUE
        System.out.println(String.format("rand member: %s", jedis.srandmember("sname"))); // rand member: h

        // 删除一个对象
        jedis.srem("sname", "t");
        System.out.println(String.format("all members: %s", jedis.smembers("sname"))); // all members: [h, w]

        // zset
        jedis.zadd("zset", 0, "car");
        jedis.zadd("zset", 2, "bike");
        Set<String> sose = jedis.zrange("zset", 0, -1);
        Iterator<String> it = sose.iterator();
        while (it.hasNext()) {
            System.out.print(it.next() + "\t"); // car bike
        }

        RedisUtil.close(jedis);
    }

    public static void testPool() {
        Jedis jedis = RedisUtil.getJedisFromPool();
        jedis.select(0);

        jedis.set("name4", "tim");
        jedis.append("name4", " is a student.");

        System.out.println(jedis.get("name4")); // tim is a student.

        RedisUtil.close(jedis);
        RedisUtil.destroyPool();
    }
}
