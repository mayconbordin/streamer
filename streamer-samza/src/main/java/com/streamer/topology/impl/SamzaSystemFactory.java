package com.streamer.topology.impl;

import com.streamer.utils.SamzaConfiguration;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;

public class SamzaSystemFactory implements SystemFactory {

    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry mr) {
        return new SamzaSystemConsumer(systemName, new SamzaConfiguration(config));
    }

    public SystemProducer getProducer(String string, Config config, MetricsRegistry mr) {
        throw new SamzaException("This implementation is not supposed to produce anything.");
    }

    public SystemAdmin getAdmin(String string, Config config) {
        return new SinglePartitionWithoutOffsetsSystemAdmin();
    }
    
}
