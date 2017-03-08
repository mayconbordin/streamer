package com.streamer.topology.impl;

import com.streamer.core.ISource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSourceInstance implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalSourceInstance.class);
    private ISource source;
    private int index;

    public LocalSourceInstance(ISource source, int index) {
        this.source = source;
        this.index = index;
    }

    public ISource getSource() {
        return source;
    }

    public int getIndex() {
        return index;
    }

    public void run() {
        while (source.hasNext()) {
            source.hooksBefore();
            source.nextTuple();
            source.hooksAfter();
        }
        
        LOG.info("Source {} finished", source.getDefaultOutputStream());
    }
}