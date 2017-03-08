package com.streamer.topology.impl;

import com.streamer.core.IOperator;
import com.streamer.core.IStream;
import com.streamer.core.hook.Hook;
import com.streamer.core.impl.S4Tuple;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.utils.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class S4OperatorAdapter extends ProcessingElement implements IOperatorAdapter {
    private IOperator operator;
    private Configuration config;
    private long timeInterval = 0L;

    public S4OperatorAdapter(App app, String name) {
        super(app);
        super.setName(name);
    }

    public void onEvent(S4Tuple tuple) {
        hooksBefore();
        operator.process(tuple.getValue());
        hooksAfter();
    }

    @Override
    protected void onTime() {
        operator.onTime();
    }

    @Override
    protected void onCreate() {
        operator.onCreate(Integer.parseInt(getId()), config);
    }

    @Override
    protected void onRemove() {
        operator.onDestroy();
    }

    public void setOperator(IOperator operator) {
        this.operator = operator;
    }

    public IOperator getOperator() {
        return operator;
    }

    public void addOutputStream(String id, IStream stream) {
        operator.addOutputStream(id, stream);
    }

    public void addOutputStream(IStream stream) {
        operator.addOutputStream(stream);
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }
    
    public KeyFinder<S4Tuple> getKeyFinder() {
        KeyFinder<S4Tuple> keyFinder = new KeyFinder<S4Tuple>() {
            @Override
            public List<String> get(S4Tuple tuple) {
                List<String> results = new ArrayList<String>();
                results.add(tuple.getKey());
                return results;
            }
        };

        return keyFinder;
    }

    public int getParallelism() {
        return operator.getParallelism();
    }

    public void setTimeInterval(long interval, TimeUnit timeUnit) {
        setTimerInterval(interval, timeUnit);
        timeInterval = timeUnit.toMillis(interval);
    }

    public boolean hasTimer() {
        return timeInterval > 0L;
    }

    public long getTimeIntervalMillis() {
        return timeInterval;
    }

    public void setTimeInterval(long intervalInSeconds) {
        setTimerInterval(intervalInSeconds, TimeUnit.SECONDS);
    }

    public void addComponentHook(Hook hook) {
        operator.addHook(hook);
    }
    
    private void hooksBefore() {
        for (Hook hook : operator.getHooks()) {
            hook.beforeTuple();
        }
    }
    
    private void hooksAfter() {
        for (Hook hook : operator.getHooks()) {
            hook.afterTuple();
        }
    }
}
