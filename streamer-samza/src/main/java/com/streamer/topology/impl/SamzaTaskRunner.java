package com.streamer.topology.impl;

import com.beust.jcommander.Parameter;
import com.streamer.topology.TaskRunner;
import com.streamer.utils.Configuration;
import com.streamer.utils.HDFSUtils;
import com.streamer.utils.SamzaConfiguration;
import static com.streamer.utils.SamzaConfiguration.*;
import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaTaskRunner extends TaskRunner {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaTaskRunner.class);
    
    @Parameter(names = "-jar-package", description = "Full path to the configuration file")
    public String jarPackagePath;

    public SamzaTaskRunner(String[] args) {
        super(args);
    }
    
    public static void main(String[] args) {
        SamzaEngine engine = SamzaEngine.getEngine();
        SamzaTaskRunner taskRunner = new SamzaTaskRunner(args);
        SamzaConfiguration config = (SamzaConfiguration) taskRunner.getConfiguration();
        SamzaComponentFactory factory = new SamzaComponentFactory();
        factory.setConfiguration(config);
        String hdfsPath = null;
        
        try {
            if (!config.isLocalMode()) {
                hdfsPath = uploadJarToHDFS(config, taskRunner.jarPackagePath);
            }
            
            taskRunner.start(factory);
            engine.submitTopology((SamzaTopology) taskRunner.getTopology());
            engine.setJarPackagePath(hdfsPath);
        
            engine.run();
        } catch (Exception ex) {
            LOG.error("Unable to execute topology " + taskRunner.getTopologyName(), ex);
        }
    }
    
    private static String uploadJarToHDFS(SamzaConfiguration config, String jarPackagePath) throws Exception {
        HDFSUtils.setHadoopConfigHome(config.getString(YARN_CONFIG_HOME));
        HDFSUtils.setSystemDir(config.getString(SAMZA_HDFS_DIR));
        
        Path path = FileSystems.getDefault().getPath(jarPackagePath);
        File file = path.toFile();
        String hdfsPath = HDFSUtils.writeToHDFS(file, file.getName());
        
        if (hdfsPath == null) {
            throw new Exception("Fail uploading JAR file \""+path.toAbsolutePath().toString()+"\" to HDFS.");
        }
        
        return hdfsPath;
    }
    
    @Override
    protected Configuration createConfiguration(String configStr) {
        if (configStr == null)
            return new SamzaConfiguration();
        else
            return new SamzaConfiguration(Configuration.fromStr(configStr));
    }
}
