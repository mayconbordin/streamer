package com.streamer.topology.impl;

import com.streamer.core.IStream;
import com.streamer.core.impl.SamzaStream;
import com.streamer.core.impl.SamzaStream.Subscriber;
import com.streamer.utils.HDFSUtils;
import com.streamer.utils.KafkaUtils;
import com.streamer.utils.SamzaConfiguration;
import static com.streamer.utils.SamzaConfiguration.*;
import com.streamer.utils.SamzaTaskConfiguration;
import java.util.List;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaEngine {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaEngine.class);
    
    private static SamzaEngine engine;
    
    private SamzaTopology topology;
    private SamzaConfiguration config;
    private boolean isLocalMode;
    private String jarPackagePath;
    
    private SamzaEngine() {}
    
    public static SamzaEngine getEngine() {
        if (engine == null) {
            engine = new SamzaEngine();
        }
        return engine;
    }
    
    public void submitTopology(SamzaTopology topology) {
        LOG.info("Topology {} submitted to SamzaEngine", topology.getName());
        
        this.topology = topology;
        this.config = (SamzaConfiguration) topology.getConfiguration();
        isLocalMode = config.isLocalMode();
        
        // Setup HDFS and Kafka
        if (!isLocalMode) {
            HDFSUtils.setHadoopConfigHome(config.getString(YARN_CONFIG_HOME));
        }
        
        KafkaUtils.setZookeeper(config.getString(ZOOKEEPER_HOSTS));
    }
    
    public void run() throws Exception {
        SamzaTaskConfiguration taskConfig = new SamzaTaskConfiguration(config);
        taskConfig.setJarPath(jarPackagePath);
        
        // Generate task configurations
        List<MapConfig> configs = taskConfig.getMapConfigsForTopology(topology);
        
        // Create kafka streams
        for (IStream stream : topology.getStreams()) {
            for (Subscriber s : ((SamzaStream)stream).getSubscribers()) {
                KafkaUtils.createKafkaTopic(s.getStream(), s.getParallelism(), config.getInt(CHECKPOINT_REPLICATION_FACTOR), true);
            }
        }

        // Submit jobs
        for (MapConfig cfg : configs) {
            LOG.info("Submitting job {} to Samza", cfg.get(SamzaTaskConfiguration.JOB_NAME_KEY));
            JobRunner jobRunner = new JobRunner(cfg);
            jobRunner.run();
        }
    }

    public void setJarPackagePath(String jarPackagePath) {
        this.jarPackagePath = jarPackagePath;
    }
}
