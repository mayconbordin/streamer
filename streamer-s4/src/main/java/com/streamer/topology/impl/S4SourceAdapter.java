package com.streamer.topology.impl;

import com.streamer.core.ISource;
import com.streamer.core.IStream;
import com.streamer.core.hook.Hook;
import com.streamer.topology.ISourceAdapter;
import com.streamer.utils.Configuration;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class S4SourceAdapter extends ProcessingElement implements ISourceAdapter {
    private ISource source;
    private Configuration config;
    private int rate;
    
    public S4SourceAdapter(App app, String name) {
        super(app);
        super.setName(name);
    }
    
    @Override
    protected void onCreate() {
        source.onCreate(Integer.parseInt(getId()), config);
    }

    @Override
    protected void onRemove() {
        source.onDestroy();
    }

    public void setSource(ISource source) {
        this.source = source;
    }

    public ISource getSource() {
        return source;
    }

    public void addOutputStream(String id, IStream stream) {
        source.addOutputStream(id, stream);
    }

    public void addOutputStream(IStream stream) {
        source.addOutputStream(stream);
    }

    public int getParallelism() {
        return source.getParallelism();
    }

    public void setTupleRate(int rate) {
        this.rate = rate;
    }

    public void addComponentHook(Hook hook) {
        source.addHook(hook);
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }
    
    private void hooksBefore() {
        for (Hook hook : source.getHooks()) {
            hook.beforeTuple();
        }
    }
    
    private void hooksAfter() {
        for (Hook hook : source.getHooks()) {
            hook.afterTuple();
        }
    }
    
    public boolean injectNextEvent() {
        if (source.hasNext()) {
            hooksBefore();
            source.nextTuple();
            hooksAfter();
            return source.hasNext();
        } else
            return false;
    }
}
