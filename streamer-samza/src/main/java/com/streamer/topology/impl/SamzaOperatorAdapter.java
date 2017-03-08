package com.streamer.topology.impl;

import com.streamer.core.IOperator;
import com.streamer.core.IStream;
import com.streamer.core.Tuple;
import com.streamer.core.hook.Hook;
import com.streamer.core.impl.SamzaStream;
import com.streamer.core.impl.SamzaStream.Subscriber;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.utils.FileSystemUtils;
import com.streamer.utils.HDFSUtils;
import com.streamer.utils.SamzaConfiguration;
import com.streamer.utils.SamzaTaskConfiguration;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

public class SamzaOperatorAdapter implements IOperatorAdapter, InitableTask, StreamTask {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaOperatorAdapter.class);
    
    private IOperator operator;
    private SamzaConfiguration config;
    private long interval = -1;
    
    public void setComponent(IOperator operator) {
        this.operator = operator;
    }

    public IOperator getComponent() {
        return operator;
    }

    public void setTimeInterval(long interval, TimeUnit timeUnit) {
        this.interval = timeUnit.toMillis(interval);
    }

    public boolean hasTimer() {
        return interval != -1;
    }

    public long getTimeIntervalMillis() {
        return interval;
    }

    public void addComponentHook(Hook hook) {
        operator.addHook(hook);
    }
    
    public List<Subscriber> getInputStreams() {
        List<Subscriber> inputStreams = new ArrayList<Subscriber>();
        
        for (IStream stream : operator.getInputStreams()) {
            Subscriber s = ((SamzaStream)stream).getSubscriber(operator.getName());
            
            if (s != null) {
                inputStreams.add(s);
            }
        }
        
        return inputStreams;
    }

    public void init(Config cfg, TaskContext context) throws Exception {
        config = new SamzaConfiguration(cfg);
        
        String yarnConfHome = config.getString(SamzaTaskConfiguration.YARN_CONF_HOME_KEY);
        if (!StringUtils.isBlank(yarnConfHome))
            HDFSUtils.setHadoopConfigHome(yarnConfHome);

        String filename   = config.getString(SamzaTaskConfiguration.FILE_KEY);
        String filesystem = config.getString(SamzaTaskConfiguration.FILESYSTEM_KEY);
        String jobName    = config.getString(SamzaTaskConfiguration.JOB_NAME_KEY);
        
        LOG.info("Retrieving serialized operator for '{}' at {}://{}", jobName, filesystem, filename);
        SerializationProxy wrapper = (SerializationProxy) FileSystemUtils.deserializeObjectFromFileAndKey(filesystem, filename, jobName);
        this.operator     = wrapper.operator;
        this.interval     = wrapper.interval;
        
        operator.onCreate(context.getPartition().getPartitionId(), config);
        
        for (IStream stream : operator.getOutputStreams().values()) {
            ((SamzaStream) stream).onCreate();
        }
    }

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
        // set collector for output streams
        
        operator.hooksBefore();
        
        setOutputStreamCollector(collector);
        operator.process((Tuple) envelope.getMessage());
        
        operator.hooksAfter();
    }
    
    /**
     * Set the message collector to all stream before sending tuple to be processed
     * @param collector 
     */
    private void setOutputStreamCollector(MessageCollector collector) {
        for (IStream stream : operator.getOutputStreams().values()) {
            ((SamzaStream)stream).setCollector(collector);
        }
    }
    
    private Object writeReplace() {
        return new SerializationProxy(this);
    }
    
    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = 3344453117992918889L;
        
        private IOperator operator;
        private long interval = -1;
        
        public SerializationProxy(SamzaOperatorAdapter adapter) {
            this.operator     = adapter.operator;
            this.interval     = adapter.interval;
        }
    }
}
