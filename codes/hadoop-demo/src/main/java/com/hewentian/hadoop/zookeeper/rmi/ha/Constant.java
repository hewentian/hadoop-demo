package com.hewentian.hadoop.zookeeper.rmi.ha;

public interface Constant {
    String ZK_CONNECTION_STRING = "hadoop-host-master:2181,hadoop-host-slave-1:2181,hadoop-host-slave-2:2181";
    int ZK_SESSION_TIMEOUT = 5000;
    String ZK_REGISTRY_PATH = "/registry";
    String ZK_PROVIDER_PATH = ZK_REGISTRY_PATH + "/provider";
}
