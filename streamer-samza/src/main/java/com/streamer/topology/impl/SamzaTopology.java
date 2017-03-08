package com.streamer.topology.impl;

import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;

public class SamzaTopology extends Topology {

    public SamzaTopology(String name, Configuration config) {
        super(name, config);
    }

    @Override
    public void finish() {
    }
    
}
