package com.streamer.utils;

import com.streamer.topology.impl.SamzaConstants.Mode;
import java.util.Map;

public class SamzaConfiguration extends Configuration {
    public static final String YARN_CONFIG_HOME = "yarn.config.home";
    
    public static final String YARN_AM_MEM = "yarn.am.mem";
    public static final Object YARN_AM_MEM_DEFAULT = 1024;
    
    public static final String YARN_CONTAINER_MEM = "yarn.container.mem";
    public static final Object YARN_CONTAINER_MEM_DEFAULT = 1024;
    
    public static final String SAMZA_MODE = "samza.mode";
    public static final Object SAMZA_MODE_DEFAULT = Mode.LOCAL;
    
    public static final String SAMZA_HDFS_DIR = "samza.hdfs.dir";
    
    public static final String ZOOKEEPER_HOSTS = "zookeeper.hosts";
    public static final Object ZOOKEEPER_HOSTS_DEFAULT = "localhost:2181";
    
    public static final String KAFKA_BROKERS = "kafka.brokers";
    public static final Object KAFKA_BROKERS_DEFAULT = "localhost:9092";
    
    public static final String CHECKPOINT_FREQUENCY = "checkpoint.frequency";
    public static final Object CHECKPOINT_FREQUECY_DEFAULT = 60000;
    
    public static final String CHECKPOINT_REPLICATION_FACTOR = "checkpoint.replication.factor";
    public static final Object CHECKPOINT_REPLICATION_FACTOR_DEFAULT = 1;
    
    public static final String CONTAINER_NUM_OPERATORS = "container.operators.num";
    public static final Object CONTAINER_NUM_OPERATORS_DEFAULT = 2;
    
    public static final String KRYO_REGISTRATION_FILE  = "kryo.register.file";

    public SamzaConfiguration() {
    }

    public SamzaConfiguration(Map<? extends String, ? extends Object> map) {
        super(map);
    }

    @Override
    public void loadDefaultValues() {
        super.loadDefaultValues();
        
        put(SAMZA_MODE, SAMZA_MODE_DEFAULT);
        put(KAFKA_BROKERS, KAFKA_BROKERS_DEFAULT);
        put(ZOOKEEPER_HOSTS, ZOOKEEPER_HOSTS_DEFAULT);
        put(YARN_AM_MEM, YARN_AM_MEM_DEFAULT);
        put(YARN_CONTAINER_MEM, YARN_CONTAINER_MEM_DEFAULT);
        put(CONTAINER_NUM_OPERATORS, CONTAINER_NUM_OPERATORS_DEFAULT);
        put(CHECKPOINT_FREQUENCY, CHECKPOINT_FREQUECY_DEFAULT);
        put(CHECKPOINT_REPLICATION_FACTOR, CHECKPOINT_REPLICATION_FACTOR_DEFAULT);
    }
    
    public boolean isLocalMode() {
        return getString(SAMZA_MODE).equals(Mode.LOCAL);
    }
}
