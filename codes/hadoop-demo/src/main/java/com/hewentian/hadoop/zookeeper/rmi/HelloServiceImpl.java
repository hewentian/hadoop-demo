package com.hewentian.hadoop.zookeeper.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * <p>
 * <b>HelloServiceImpl</b> 是
 * </p>
 *
 * @author <a href="mailto:wentian.he@qq.com">hewentian</a>
 * @date 2019-02-21 09:40:02
 * @since JDK 1.8
 */
public class HelloServiceImpl extends UnicastRemoteObject implements HelloService {
    public HelloServiceImpl() throws RemoteException {
    }

    @Override
    public String sayHello(String name) throws RemoteException {
        return String.format("Hello %s", name);
    }
}
