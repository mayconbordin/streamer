package com.streamer.core.impl;

import com.streamer.core.IComponent;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.core.Tuple;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.impl.S4OperatorAdapter;
import com.streamer.topology.impl.S4App;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 *
 * @author mayconbordin
 */
public class S4Stream extends Stream {
    private S4App app;
    private Map<String, Subscriber> subscribers;
    private int shuffleCounter;

    public S4Stream(S4App app, String id, Schema schema) {
        super(id, schema);
        this.app = app;
        shuffleCounter = 0;
        subscribers = new HashMap<String, Subscriber>();
    }
    
    @Override
    public void addSubscriber(String name, IOperatorAdapter adapter, PartitioningScheme partitioning) {
        addSubscriber(name, adapter, partitioning, null);
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter adapter, PartitioningScheme partitioning, Fields fields) {
        String streamName = String.format("%s_%s", id, name);
        org.apache.s4.core.Stream<S4Tuple> stream = app.createStream(streamName, (S4OperatorAdapter)adapter);
        stream.setName(streamName);
        stream.setKey(((S4OperatorAdapter)adapter).getKeyFinder());
        subscribers.put(name, new Subscriber(stream, adapter, partitioning, fields));
    }

    @Override
    public void put(IComponent component, Tuple tuple) {
        for (Subscriber s : subscribers.values()) {
            switch (s.getPartitioning()) {
                case SHUFFLE:
                    S4Tuple s4tuple = new S4Tuple(tuple);
                    s4tuple.setStreamId(s.getStream().getName());
                    if (s.getAdapter().getParallelism() == 1) {
                        s4tuple.setKey("0");
                    } else {
                        s4tuple.setKey(Integer.toString(shuffleCounter));
                    }
                
                    s.getStream().put(s4tuple);
                    shuffleCounter++;
                    if (shuffleCounter >= s.getAdapter().getParallelism()) {
                        shuffleCounter = 0;
                    }
                    break;
                
                case GROUP_BY:
                    S4Tuple s4tuple1 = new S4Tuple(tuple);
                    s4tuple1.setStreamId(s.getStream().getName());
                    HashCodeBuilder hb = new HashCodeBuilder();
                    
                    if (s.getFields() != null) {
                        for (String key : s.getFields())
                            hb.append(tuple.get(key));
                    } else {
                        for (String key : schema.getKeys())
                            hb.append(tuple.get(key));
                    }
                    
                    String key = Integer.toString(hb.build() % s.getAdapter().getParallelism());
                    s4tuple1.setKey(key);
                    s.getStream().put(s4tuple1);
                    break;
                    
                case BROADCAST:
                    for (int p = 0; p < s.getAdapter().getParallelism(); p++) {
                        S4Tuple s4tuple2 = new S4Tuple(tuple);
                        s4tuple2.setStreamId(s.getStream().getName());
                        s4tuple2.setKey(Integer.toString(p));
                        s.getStream().put(s4tuple2);
                    }
                
                default:
                    break;
            }
        }
    }
    
    
    protected static class Subscriber {
        private org.apache.s4.core.Stream stream;
        private IOperatorAdapter adapter;
        private PartitioningScheme partitioning;
        private Fields fields;

        public Subscriber(org.apache.s4.core.Stream stream, IOperatorAdapter adapter, PartitioningScheme partitioning, Fields fields) {
            this.stream = stream;
            this.adapter = adapter;
            this.partitioning = partitioning;
            this.fields = fields;
        }

        public org.apache.s4.core.Stream getStream() {
            return stream;
        }

        public IOperatorAdapter getAdapter() {
            return adapter;
        }

        public PartitioningScheme getPartitioning() {
            return partitioning;
        }

        public Fields getFields() {
            return fields;
        }
    }
}
