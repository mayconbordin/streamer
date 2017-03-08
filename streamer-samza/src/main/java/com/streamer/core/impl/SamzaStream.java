package com.streamer.core.impl;

import com.streamer.core.IComponent;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.core.Tuple;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.impl.SamzaConstants;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaStream extends Stream {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaStream.class);
    
    private List<Subscriber> subscribers;
    private Map<String, Subscriber> subscriberMap;
    
    private String system;
    private transient MessageCollector collector;
    private int batchSize;
    
    public SamzaStream(String id, Schema schema) {
        super(id, schema);
        
        system        = SamzaConstants.DEFAULT_SYSTEM_NAME;
        subscribers   = new ArrayList<Subscriber>();
        subscriberMap = new HashMap<String, Subscriber>();
        batchSize     = 1;
    }
    
    public void onCreate() {
        for (Subscriber s : subscribers) {
            s.initSystemStream();
        }
    }

    public void put(IComponent component, Tuple tuple) {
        for (Subscriber s : subscribers) {
            switch (s.getPartitioning()) {
                case SHUFFLE:
                    collector.send(new OutgoingMessageEnvelope(s.getSystemStream(), tuple));
                    break;
                    
                case GROUP_BY:
                    String keyStr = "";
                    if (s.getFields() != null) {
                        for (String key : s.getFields())
                            keyStr += tuple.get(key);
                    } else {
                        for (String key : schema.getKeys())
                            keyStr += tuple.get(key);
                    }
                    
                    collector.send(new OutgoingMessageEnvelope(s.getSystemStream(), keyStr, null, tuple));
                    break;
                    
                case BROADCAST:
                    for (int p = 0; p < s.getParallelism(); p++) {
                        collector.send(new OutgoingMessageEnvelope(s.getSystemStream(), p, null, tuple));
                    }
                    break;
            }
        }
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning, Fields fields) {
        Subscriber s = getSubscriber(partitioning, op.getComponent().getParallelism(), fields);
        
        if (s == null) {
            String topicName = id + "-" + Integer.toString(subscribers.size());
            s = new Subscriber(system, topicName, partitioning, fields, op.getComponent().getParallelism());
            subscribers.add(s);
        }
        
        subscriberMap.put(name, s);
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning) {
        addSubscriber(name, op, partitioning, null);
    }
    
    public Subscriber getSubscriber(String name) {
        return subscriberMap.get(name);
    }

    public void setCollector(MessageCollector collector) {
        this.collector = collector;
    }

    public void setSystem(String system) {
        this.system = system;
        
        for (Subscriber s : subscribers) {
            s.setSystem(system);
        }
    }

    public String getSystem() {
        return system;
    }
    
    public List<Subscriber> getSubscribers() {
        return subscribers;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
    
    private Subscriber getSubscriber(PartitioningScheme partitioning, int parallelism, Fields fields) {
        for (Subscriber s : subscribers) {
            if (s.isSame(partitioning, parallelism, fields))
                return s;
        }
        
        return null;
    }
    
    public static class Subscriber implements Serializable {
        private PartitioningScheme partitioning;
        private Fields fields;
        private String system;
        private String stream;
        private int parallelism;
        
        private transient SystemStream systemStream = null;
        
        public Subscriber(String system, String stream, PartitioningScheme partitioning, Fields fields, int parallelism) {
            this.system       = system;
            this.stream       = stream;
            this.parallelism  = parallelism;
            this.partitioning = partitioning;
            this.fields       = fields;
        }
        
        public void initSystemStream() {
            LOG.info("Initializing stream {} for system {} with parallelism {} and partitioning {}", stream, system, parallelism, partitioning.toString());
            systemStream = new SystemStream(system, stream);
        }

        public void setSystem(String system) {
            this.system = system;
        }

        public int getParallelism() {
            return parallelism;
        }

        public PartitioningScheme getPartitioning() {
            return partitioning;
        }

        public Fields getFields() {
            return fields;
        }

        public String getSystem() {
            return system;
        }

        public String getStream() {
            return stream;
        }

        public SystemStream getSystemStream() {
            return systemStream;
        }
        
        public boolean isSame(PartitioningScheme partitioning, int parallelism, Fields fields) {
            return (partitioning == this.partitioning && parallelism == this.parallelism && fields.containsAll(this.fields));
        }
    }
}
