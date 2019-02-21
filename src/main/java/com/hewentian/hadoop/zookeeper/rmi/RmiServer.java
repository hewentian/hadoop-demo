package com.hewentian.hadoop.zookeeper.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 * <p>
 * <b>RmiServer</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 09:42:28
 * @since JDK 1.8
 */
public class RmiServer {
    public static void main(String[] args) throws RemoteException, MalformedURLException {
        int port = 12345;
        String url = "rmi://localhost:" + port + "/com.hewentian.hadoop.zookeeper.rmi.HelloServiceImpl";
        LocateRegistry.createRegistry(port);
        Naming.rebind(url, new HelloServiceImpl());
    }
}
