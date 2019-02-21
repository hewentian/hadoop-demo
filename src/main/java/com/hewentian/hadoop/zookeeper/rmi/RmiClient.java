package com.hewentian.hadoop.zookeeper.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * <p>
 * <b>RmiClient</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 09:48:56
 * @since JDK 1.8
 */
public class RmiClient {
    public static void main(String[] args) throws RemoteException, NotBoundException, MalformedURLException {
        int port = 12345;
        String url = "rmi://localhost:" + port + "/com.hewentian.hadoop.zookeeper.rmi.HelloServiceImpl";

        HelloService helloService = (HelloService) Naming.lookup(url);
        String result = helloService.sayHello("Tim");
        System.out.println(result);
    }
}
