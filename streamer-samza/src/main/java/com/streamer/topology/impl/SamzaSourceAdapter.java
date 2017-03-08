package com.streamer.topology.impl;

import com.streamer.core.ISource;
import com.streamer.core.IStream;
import com.streamer.core.Tuple;
import com.streamer.core.hook.Hook;
import com.streamer.core.impl.SamzaStream;
import com.streamer.topology.ISourceAdapter;
import com.streamer.utils.FileSystemUtils;
import com.streamer.utils.HDFSUtils;
import com.streamer.utils.SamzaConfiguration;
import com.streamer.utils.SamzaTaskConfiguration;
import java.io.Serializable;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaSourceAdapter implements ISourceAdapter, InitableTask, StreamTask {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaSourceAdapter.class);
    
    private ISource source;
    private SamzaConfiguration config;
    private int rate;
    
    public void setComponent(ISource source) {
        this.source = source;
    }

    public ISource getComponent() {
        return source;
    }

    public void setTupleRate(int rate) {
        this.rate = rate;
    }

    public void addComponentHook(Hook hook) {
        source.addHook(hook);
    }

    public void init(Config cfg, TaskContext context) throws Exception {
        config = new SamzaConfiguration(cfg);
        
        String yarnConfHome = config.getString(SamzaTaskConfiguration.YARN_CONF_HOME_KEY);
        if (!StringUtils.isBlank(yarnConfHome))
            HDFSUtils.setHadoopConfigHome(yarnConfHome);

        String filename   = config.getString(SamzaTaskConfiguration.FILE_KEY);
        String filesystem = config.getString(SamzaTaskConfiguration.FILESYSTEM_KEY);
        String jobName    = config.getString(SamzaTaskConfiguration.JOB_NAME_KEY);
        
        LOG.info("Retrieving serialized source for '{}' at {}://{}", jobName, filesystem, filename);
        SerializationProxy wrapper = (SerializationProxy) FileSystemUtils.deserializeObjectFromFileAndKey(filesystem, filename, jobName);
        this.source = wrapper.source;
        this.rate   = wrapper.rate;
        
        source.onCreate(context.getPartition().getPartitionId(), config);
        
        for (IStream stream : source.getOutputStreams().values()) {
            ((SamzaStream) stream).onCreate();
        }
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        Tuple tuple = (Tuple) envelope.getMessage();
        SamzaStream stream = (SamzaStream) source.getOutputStreams().get(tuple.getStreamId());
        stream.setCollector(collector);
        
        source.hooksBefore();
        stream.put(source, tuple);
        source.hooksAfter();
    }
    
    private Object writeReplace() {
        return new SerializationProxy(this);
    }
    
    public static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = -5332020480184471106L;
        
        private ISource source;
        private int rate;
        
        public SerializationProxy(SamzaSourceAdapter adapter) {
            this.source = adapter.source;
            this.rate   = adapter.rate;
        }

        public ISource getSource() {
            return source;
        }

        public int getRate() {
            return rate;
        }
    }
}
