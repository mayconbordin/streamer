package com.streamer.topology.impl;

import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Stream;
import com.streamer.core.IOperator;
import com.streamer.core.ISource;
import com.streamer.core.IStream;
import com.streamer.core.Schema;
import com.streamer.topology.ComponentFactory;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.Topology;
import com.streamer.core.impl.S4Stream;
import com.streamer.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class S4ComponentFactory implements ComponentFactory {
    private S4App app;
    private Configuration config;
    private MetricRegistry metrics;

    public S4ComponentFactory(S4App app) {
        this.app = app;
    }
    
    public IStream createStream(String name, Schema schema) {
        return new S4Stream(app, name, schema);
    }

    public IOperatorAdapter createOperatorAdapter(String name, IOperator operator) {
        IOperatorAdapter adapter = new S4OperatorAdapter(app, name);
        adapter.setOperator(operator);
        adapter.setConfiguration(config);
        return adapter;
    }

    public ISourceAdapter createSourceAdapter(String name, ISource source) {
        ISourceAdapter adapter = new S4SourceAdapter(app, name);
        adapter.setSource(source);
        adapter.setConfiguration(config);
        return adapter;
    }

    public Topology createTopology(String name) {
        return new S4Topology(name);
    }

    public void setMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void setConfiguration(Configuration configuration) {
        this.config = configuration;
    }
    
}
