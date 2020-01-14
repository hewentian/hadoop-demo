package com.hewentian.hadoop.utils;

import java.io.*;

/**
 * <p>
 * <b>CommonUtil</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-03-07 20:24:23
 * @since JDK 1.8
 */
public class CommonUtil {
    private CommonUtil() {
    }

    /**
     * java对象转化为字节流
     **/
    public static byte[] serialize(Object obj) {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;

        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);

            oos.writeObject(obj);

            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                oos.close();
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 反编译 java字节流对象
     **/
    public static Object deSerialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        ObjectInputStream ois = null;

        try {
            bais = new ByteArrayInputStream(bytes);
            ois = new ObjectInputStream(bais);

            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                ois.close();
                bais.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
