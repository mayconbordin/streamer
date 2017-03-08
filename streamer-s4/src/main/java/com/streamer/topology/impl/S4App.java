package com.streamer.topology.impl;

import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.streamer.core.Task;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.TaskRunner;
import com.streamer.topology.Topology;
import com.streamer.topology.TopologyBuilder;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class S4App extends App {
    @Inject
    @Named("evalTask")
    public String evalTask;
    
    private Topology topology;
    private TaskRunner taskRunner;
    
    @Override
    protected void onInit() {
        System.out.println("Starting application...");
        System.out.println("Task: " + evalTask);
        taskRunner = new TaskRunner(evalTask.split(" "));
        
        S4ComponentFactory factory = new S4ComponentFactory(this);
        factory.setConfiguration(taskRunner.getConfiguration());
        
        taskRunner.start(factory);
        topology = taskRunner.getTopology();
    }
    
    @Override
    protected void onStart() {
        System.out.println("Starting application");
        // start sources
        // num of threads = parallelism ?
        
        S4SourceAdapter adapter = (S4SourceAdapter) topology.getComponent("source");
        while (adapter.injectNextEvent())
            // inject events
            ;
    }

    @Override
    protected void onClose() {
    }
    
    @Override
    public <T extends ProcessingElement> T createPE(Class<T> type) {
        return super.createPE(type);
    }

    @Override
    public <T extends Event> Stream<T> createStream(String name, KeyFinder<T> finder, ProcessingElement... processingElements) {
        return super.createStream(name, finder, processingElements);
    }

    @Override
    public <T extends Event> Stream<T> createStream(String name, ProcessingElement... processingElements) {
        return super.createStream(name, processingElements);
    }
}
