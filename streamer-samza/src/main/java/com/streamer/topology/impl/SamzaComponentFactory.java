package com.streamer.topology.impl;

import com.streamer.core.IOperator;
import com.streamer.core.ISource;
import com.streamer.core.IStream;
import com.streamer.core.Schema;
import com.streamer.core.impl.SamzaStream;
import com.streamer.topology.ComponentFactory;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;
import com.streamer.utils.SamzaConfiguration;

public class SamzaComponentFactory implements ComponentFactory {
    private SamzaConfiguration configuration;
    
    public IStream createStream(String name, Schema schema) {
        return new SamzaStream(name, schema);
    }

    public IOperatorAdapter createOperatorAdapter(String name, IOperator operator) {
        SamzaOperatorAdapter adapter = new SamzaOperatorAdapter();
        adapter.setComponent(operator);
        
        return adapter;
    }

    public ISourceAdapter createSourceAdapter(String name, ISource source) {
        SamzaSourceAdapter adapter = new SamzaSourceAdapter();
        adapter.setComponent(source);
        
        return adapter;
    }

    public Topology createTopology(String name) {
        return new SamzaTopology(name, configuration);
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = (SamzaConfiguration) configuration;
    }
    
}
