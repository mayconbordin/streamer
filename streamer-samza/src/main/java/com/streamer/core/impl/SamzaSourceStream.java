package com.streamer.core.impl;

import com.streamer.core.IComponent;
import com.streamer.core.Schema;
import com.streamer.core.Tuple;
import com.streamer.topology.impl.SamzaSystemConsumer;

public class SamzaSourceStream extends SamzaStream {
    private SamzaSystemConsumer consumer;

    public SamzaSourceStream(String id, Schema schema) {
        super(id, schema);
    }

    @Override
    public void put(IComponent component, Tuple tuple) {
        consumer.put(tuple);
    }

    public void setConsumer(SamzaSystemConsumer consumer) {
        this.consumer = consumer;
    }
}