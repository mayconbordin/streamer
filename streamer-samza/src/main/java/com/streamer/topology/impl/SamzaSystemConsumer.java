package com.streamer.topology.impl;

import com.streamer.core.ISource;
import com.streamer.core.IStream;
import com.streamer.core.Tuple;
import com.streamer.core.impl.SamzaSourceStream;
import com.streamer.topology.impl.SamzaSourceAdapter.SerializationProxy;
import com.streamer.utils.FileSystemUtils;
import com.streamer.utils.HDFSUtils;
import com.streamer.utils.SamzaConfiguration;
import com.streamer.utils.SamzaTaskConfiguration;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaSystemConsumer extends BlockingEnvelopeMap {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaSystemConsumer.class);
    private static final int QUEUE_SIZE  = 10000;
    private static final int QUEUE_DELAY = 100;
    
    private ISource source;
    private SystemStreamPartition systemStreamPartition;
    private Thread thread;

    public SamzaSystemConsumer(String systemName, SamzaConfiguration config) {
        String yarnConfHome = config.getString(SamzaTaskConfiguration.YARN_CONF_HOME_KEY);
        if (!StringUtils.isBlank(yarnConfHome))
            HDFSUtils.setHadoopConfigHome(yarnConfHome);

        String filename   = config.getString(SamzaTaskConfiguration.FILE_KEY);
        String filesystem = config.getString(SamzaTaskConfiguration.FILESYSTEM_KEY);
        String jobName    = config.getString(SamzaTaskConfiguration.JOB_NAME_KEY);
        
        SerializationProxy wrapper = (SerializationProxy) FileSystemUtils.deserializeObjectFromFileAndKey(filesystem, filename, jobName);
        source = wrapper.getSource();
        source.onCreate(0, config);
        
        systemStreamPartition = new SystemStreamPartition(systemName, source.getName(), new Partition(0));
        
        prepareOutputStreams();
        
        // remove hooks
        source.getHooks().clear();
    }
    
    private void prepareOutputStreams() {
        Map<String, IStream> outputStreams = new HashMap<String, IStream>();
        
        // replace stream objects
        for (Map.Entry<String, IStream> e : source.getOutputStreams().entrySet()) {
            outputStreams.put(e.getKey(), new SamzaSourceStream(e.getValue().getStreamId(), e.getValue().getSchema()));
        }
        
        // set consumer
        for (IStream stream : outputStreams.values()) {
            ((SamzaSourceStream)stream).setConsumer(this);
        }
        
        // override output streams
        source.setOutputStreams(outputStreams);
    }

    public void start() {
        thread = new Thread(
            new Runnable() {
                @Override
                public void run() {
                    try {
                        pollSource();
                        setIsAtHead(systemStreamPartition, true);
                    } catch (Exception ex) {
                        LOG.error("Unable to get data from source " + source.getFullName(), ex);
                        stop();
                    }
                }
            }
        );

        thread.start();
    }

    public void stop() {
        thread.interrupt();
    }
    
    private void pollSource() {
        int messageCnt = 0;
        
        while (source.hasNext()) {
            messageCnt = this.getNumMessagesInQueue(systemStreamPartition);
            
            if (messageCnt < QUEUE_SIZE) {
                source.nextTuple();
            } else {
                try {
                    Thread.sleep(QUEUE_DELAY);
                } catch (InterruptedException ex) {
                    break;
                }
            }
        }
    }
    
    public void put(Tuple tuple) {
        try {
            put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, null, null, tuple));
        } catch (InterruptedException ex) {
            LOG.error("An error occurred while storing the message from source " + source.getFullName(), ex);
            stop();
        }
    }
    
}
