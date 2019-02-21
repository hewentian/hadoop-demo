package com.hewentian.hadoop.zookeeper.rmi.ha;

import com.hewentian.hadoop.zookeeper.rmi.HelloService;
import com.hewentian.hadoop.zookeeper.rmi.HelloServiceImpl;

/**
 * <p>
 * <b>Server</b> 是 发布服务
 * 可以运行多个RMI服务，只要端口不同即可，如下运行两个
 * java Server localhost 12345
 * java Server localhost 23456
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 16:00:42
 * @since JDK 1.8
 */
public class Server {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("please using command: java Server <rmi_host> <rmi_port>");
            System.exit(-1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        ServiceProvider provider = new ServiceProvider();
        HelloService helloService = new HelloServiceImpl();

        provider.publish(helloService, host, port);

        Thread.sleep(Long.MAX_VALUE);
    }
}
