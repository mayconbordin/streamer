package com.streamer.utils;

import com.streamer.core.IStream;
import com.streamer.core.impl.SamzaStream;
import com.streamer.core.impl.SamzaStream.Subscriber;
import com.streamer.topology.IComponentAdapter;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import static com.streamer.topology.impl.SamzaConstants.*;
import com.streamer.topology.impl.SamzaOperatorAdapter;
import com.streamer.topology.impl.SamzaSourceAdapter;
import com.streamer.topology.impl.SamzaSystemFactory;
import com.streamer.topology.impl.SamzaTopology;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.local.LocalJobFactory;
import org.apache.samza.job.yarn.YarnJobFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaTaskConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaTaskConfiguration.class);
    
    // DELIMINATORS
    public static final String COMMA       = ",";
    public static final String COLON       = ":";
    public static final String DOT         = ".";
    public static final char DOLLAR_SIGN   = '$';
    public static final char QUESTION_MARK = '?';
        
    // JOB
    public static final String JOB_FACTORY_CLASS_KEY   = "job.factory.class";
    public static final String JOB_NAME_KEY            = "job.name";
    // YARN 
    public static final String YARN_PACKAGE_KEY        = "yarn.package.path";
    public static final String CONTAINER_MEMORY_KEY    = "yarn.container.memory.mb";
    public static final String AM_MEMORY_KEY           = "yarn.am.container.memory.mb";
    public static final String CONTAINER_COUNT_KEY     = "yarn.container.count";
    // TASK (SAMZA original)
    public static final String TASK_CLASS_KEY          = "task.class";
    public static final String TASK_INPUTS_KEY         = "task.inputs";
    // TASK (extra)
    public static final String FILE_KEY                = "task.processor.file";
    public static final String FILESYSTEM_KEY          = "task.processor.filesystem";
    public static final String ENTRANCE_INPUT_KEY      = "task.entrance.input";
    public static final String ENTRANCE_OUTPUT_KEY     = "task.entrance.outputs";
    public static final String YARN_CONF_HOME_KEY      = "yarn.config.home";
    // KAFKA
    public static final String ZOOKEEPER_URI_KEY       = "consumer.zookeeper.connect";
    public static final String BROKER_URI_KEY          = "producer.metadata.broker.list";
    public static final String KAFKA_BATCHSIZE_KEY     = "producer.batch.num.messages";
    public static final String KAFKA_PRODUCER_TYPE_KEY = "producer.producer.type";
    // SERDE
    public static final String SERDE_REGISTRATION_KEY  = "kryo.register";
    
    private SamzaConfiguration config;
    
    private boolean isLocalMode;
    private String zookeeper;
    private String kafkaBrokers;
    
    private int yarnAmMemory;
    private int yarnContainerMemory;
    private int containerNumOps;
    
    private int checkpointFrequency; // in ms
    private int checkpointRepFactor;

    private String jarPath;
    private String kryoRegisterFile = null;

    public SamzaTaskConfiguration(SamzaConfiguration config) {
        this.config = config;
        
        isLocalMode         = config.isLocalMode();
        zookeeper           = config.getString(SamzaConfiguration.ZOOKEEPER_HOSTS);
        kafkaBrokers        = config.getString(SamzaConfiguration.KAFKA_BROKERS);
        checkpointRepFactor = config.getInt(SamzaConfiguration.CHECKPOINT_REPLICATION_FACTOR);
        checkpointFrequency = config.getInt(SamzaConfiguration.CHECKPOINT_FREQUENCY);
        yarnAmMemory        = config.getInt(SamzaConfiguration.YARN_AM_MEM);
        yarnContainerMemory = config.getInt(SamzaConfiguration.YARN_CONTAINER_MEM);
        containerNumOps     = config.getInt(SamzaConfiguration.CONTAINER_NUM_OPERATORS);
        kryoRegisterFile    = config.getString(SamzaConfiguration.KRYO_REGISTRATION_FILE);
    }

    public void setJarPath(String jarPath) {
        this.jarPath = jarPath;
    }
    
    
    private Map<String, String> getMapForOperator(SamzaOperatorAdapter adapter, String filename, String filesystem) {
        LOG.info("Creating task configuration for operator '{}' stored at {}://{}", adapter.getComponent().getName(), filesystem, filename);
        Map<String, String> map = getBasicSystemConfig();
        
        // Set job name, task class, task inputs
        setJobName(map, adapter.getComponent().getName());
        setTaskClass(map, SamzaOperatorAdapter.class.getName());
        setTaskInputs(map, serializeInputStreams(adapter.getInputStreams()));
        
        // Component file
        setFileName(map, filename);
        setFileSystem(map, filesystem);
        
        // Configure input and output kafka systems
        List<String> kafkaSystems = new ArrayList<String>();
        configureKafka(map, adapter.getComponent().getInputStreams(), kafkaSystems);
        configureKafka(map, adapter.getComponent().getOutputStreams().values(), kafkaSystems);
        
        // Checkpointing
        /*
        setValue(map, "task.checkpoint.factory", "org.apache.samza.checkpoint.kafka.KafkaCheckpointManagerFactory");
        setValue(map, "task.checkpoint.system", "kafka0");
        setValue(map, "task.commit.ms", "1000");
        setValue(map, "task.checkpoint.replication.factor", Integer.toString(checkpointRepFactor));
        */
        
        // Number of containers
        setNumberOfContainers(map, adapter.getComponent().getParallelism(), containerNumOps);
        
        return map;
    }
    
    private Map<String, String> getMapForSource(SamzaSourceAdapter adapter, String filename, String filesystem) {
        LOG.info("Creating task configuration for source '{}' stored at {}://{}", adapter.getComponent().getName(), filesystem, filename);
        Map<String, String> map = getBasicSystemConfig();
        
         // Set job name, task class, task inputs
        setJobName(map, adapter.getComponent().getName());
        setTaskClass(map, SamzaSourceAdapter.class.getName());
        setTaskInputs(map, SYSTEM_NAME+"."+adapter.getComponent().getName());
        
        // Component file
        setFileName(map, filename);
        setFileSystem(map, filesystem);
        
        // Configure output kafka systems
        List<String> kafkaSystems = new ArrayList<String>();
        configureKafka(map, adapter.getComponent().getOutputStreams().values(), kafkaSystems);
        
        // Set samoa system factory
        setValue(map, "systems."+SYSTEM_NAME+".samza.factory", SamzaSystemFactory.class.getName());
        
        // Number of containers
        setNumberOfContainers(map, 1, containerNumOps);
        
        return map;
    }
    
    public List<Map<String,String>> getMapsForTopology(SamzaTopology topology) throws Exception {
        LOG.info("Creating configuration for topology {}", topology.getName());
        
        List<Map<String,String>> maps = new ArrayList<Map<String,String>>();
        
        // File to write serialized objects
        String filename = topology.getName()+ ".dat";
        Path dirPath    = FileSystems.getDefault().getPath("dat");
        Path filePath   = FileSystems.getDefault().getPath(dirPath.toString(), filename);
        String dstPath  = filePath.toString();
        String resPath;
        String filesystem;
        
        if (isLocalMode) {
            filesystem = FileSystemUtils.LOCAL_FS;
            File dir = dirPath.toFile();
            if (!dir.exists()) 
                org.apache.commons.io.FileUtils.forceMkdir(dir);
        }
        else {
            filesystem = FileSystemUtils.HDFS;
        }
        
        // Correct system name for streams
        setSystemNameForStreams(topology.getStreams());
        
        // Add components to map
        Map<String, Object> components = new HashMap<String, Object>();
        for (ISourceAdapter a : topology.getSources()) {
            SamzaSourceAdapter adapter = (SamzaSourceAdapter) a;
            components.put(adapter.getComponent().getName(), adapter);
        }
        for (IOperatorAdapter a : topology.getOperators()) {
            SamzaOperatorAdapter adapter = (SamzaOperatorAdapter) a;
            components.put(adapter.getComponent().getName(), adapter);
        }
        
        // Serialize components
        boolean serialized = false;
        if (isLocalMode) {
            serialized = LocalFSUtils.serializeObject(components, dstPath);
            resPath = dstPath;
        } else {
            resPath = HDFSUtils.serializeObject(components, dstPath);
            serialized = resPath != null;
        }

        if (!serialized) {
            throw new Exception("Failed serialize map of components to file");
        }
        
        // Add config for components
        for (IComponentAdapter adapter : topology.getComponents()) {
            if (adapter instanceof SamzaOperatorAdapter)
                maps.add(getMapForOperator((SamzaOperatorAdapter) adapter, resPath, filesystem));
            else if (adapter instanceof SamzaSourceAdapter)
                maps.add(getMapForSource((SamzaSourceAdapter) adapter, resPath, filesystem));
            else
                throw new Exception("Invalid adapter in topology");
        }
        
        return maps;
    }
    
    public List<MapConfig> getMapConfigsForTopology(SamzaTopology topology) throws Exception {
            List<MapConfig> configs = new ArrayList<MapConfig>();
            List<Map<String,String>> maps = getMapsForTopology(topology);
            
            for(Map<String, String> map : maps) {
                configs.add(new MapConfig(map));
            }
            
            return configs;
    }
    
    private void configureKafka(Map<String, String> map, Collection<IStream> streams, List<String> kafkaSystems) {
        for (IStream s : streams) {
            SamzaStream stream = (SamzaStream) s;
            boolean found = false;
            for (String system : kafkaSystems) {
                if (stream.getSystem().equals(system)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                kafkaSystems.add(stream.getSystem());
                setKafkaSystem(map, stream.getSystem(), zookeeper, kafkaBrokers, stream.getBatchSize());
            }
        }
    }
    
    private Map<String,String> getBasicSystemConfig() {
        Map<String, String> map = new HashMap<String, String>();
        
        // Add all configuration from the application
        map.putAll(config.toStringMap());
        
        // Job & Task
        if (isLocalMode) {
            map.put(JOB_FACTORY_CLASS_KEY, LocalJobFactory.class.getName());
        } else {
            map.put(JOB_FACTORY_CLASS_KEY, YarnJobFactory.class.getName());

            // yarn
            map.put(YARN_PACKAGE_KEY, jarPath);
            map.put(CONTAINER_MEMORY_KEY, Integer.toString(yarnContainerMemory));
            map.put(AM_MEMORY_KEY, Integer.toString(yarnAmMemory));
            map.put(CONTAINER_COUNT_KEY, "1"); 
            map.put(YARN_CONF_HOME_KEY, HDFSUtils.getHadoopConfigHome());

            // Task opts (Heap size = 0.75 container memory) 
            int heapSize = (int)(0.75 * yarnContainerMemory);
            map.put("task.opts", "-Xmx"+Integer.toString(heapSize)+"M -XX:+PrintGCDateStamps");
        }

        map.put(JOB_NAME_KEY, "");
        map.put(TASK_CLASS_KEY, "");
        map.put(TASK_INPUTS_KEY, "");

        // register serializer
        map.put("serializers.registry.kryo.class", SamzaKryoSerdeFactory.class.getName());

        // Serde registration
        setKryoRegistration(map, kryoRegisterFile);

        return map;
    }
    
    private void setSystemNameForStreams(Collection<IStream> streams) {
        Map<Integer, String> batchSizeMap = new HashMap<Integer, String>();
        batchSizeMap.put(1, "kafka0"); // default system with sync producer
        
        int counter = 0;
        for (IStream stream : streams) {
            SamzaStream samzaStream = (SamzaStream) stream;
            String systemName = batchSizeMap.get(samzaStream.getBatchSize());
            
            if (systemName == null) {
                counter++;
                // Add new system
                systemName = "kafka"+Integer.toString(counter);
                batchSizeMap.put(samzaStream.getBatchSize(), systemName);
            }
            
            samzaStream.setSystem(systemName);
        }
    }
    
    private static void setKafkaSystem(Map<String,String> map, String systemName, String zk, String brokers, int batchSize) {
        map.put("systems."+systemName+".samza.factory", KafkaSystemFactory.class.getName());
        map.put("systems."+systemName+".samza.msg.serde", "kryo");

        map.put("systems."+systemName+"."+ZOOKEEPER_URI_KEY, zk);
        map.put("systems."+systemName+"."+BROKER_URI_KEY, brokers);
        map.put("systems."+systemName+"."+KAFKA_BATCHSIZE_KEY, Integer.toString(batchSize));
        
        // remove if doesn't work
        //map.put("systems."+systemName+".samza.reset.offset", Boolean.TRUE.toString());

        map.put("systems."+systemName+".samza.offset.default", "oldest");

        if (batchSize > 1) {
            map.put("systems."+systemName+"."+KAFKA_PRODUCER_TYPE_KEY, "async");
        }
        else {
            map.put("systems."+systemName+"."+KAFKA_PRODUCER_TYPE_KEY, "sync");
        }
    }
    
    private static void setNumberOfContainers(Map<String, String> map, int parallelism, int opPerContainer) {
        int res = parallelism / opPerContainer;
        if (parallelism % opPerContainer != 0) res++;
        map.put(CONTAINER_COUNT_KEY, Integer.toString(res));
    }
    
    private static void setKryoRegistration(Map<String, String> map, String kryoRegisterFile) {
        if (kryoRegisterFile != null) {
            String value = readKryoRegistration(kryoRegisterFile);
            map.put(SERDE_REGISTRATION_KEY, value);
        }
    }
    
    private static void setJobName(Map<String,String> map, String jobName) {
        map.put(JOB_NAME_KEY, jobName);
    }

    private static void setFileName(Map<String,String> map, String filename) {
        map.put(FILE_KEY, filename);
    }

    private static void setFileSystem(Map<String,String> map, String filesystem) {
        map.put(FILESYSTEM_KEY, filesystem);
    }

    private static void setTaskClass(Map<String,String> map, String taskClass) {
        map.put(TASK_CLASS_KEY, taskClass);
    }

    private static void setTaskInputs(Map<String,String> map, String inputs) {
        map.put(TASK_INPUTS_KEY, inputs);
    }
    
    private static void setValue(Map<String,String> map, String key, String value) {
        map.put(key,value);
    }
    
    private static String serializeInputStreams(List<Subscriber> inputStreams) {
        String streamNames = "";
        boolean first = true;
        for (Subscriber s : inputStreams) {
            if (!first) streamNames += COMMA;
            streamNames += s.getSystem() + DOT + s.getStream();
            if (first) first = false;
        }
        
        return streamNames;
    }
    
    private static String readKryoRegistration(String filePath) {
        FileInputStream fis  = null;
        Properties props     = new Properties();
        StringBuilder result = new StringBuilder();
        
        try {
            fis = new FileInputStream(filePath);
            props.load(fis);
            
            boolean first = true;
            String value  = null;
            
            for (String key : props.stringPropertyNames()) {
                if (!first) result.append(COMMA);
                else first = false;
                
                result.append(key.trim().replace(DOLLAR_SIGN, QUESTION_MARK));
                value = props.getProperty(key);
                
                if (value != null && value.trim().length() > 0) {
                    result.append(COLON);
                    result.append(value.trim().replace(DOLLAR_SIGN, QUESTION_MARK));
                }
            }
        } catch (FileNotFoundException ex) {
            LOG.error("Kryo registration file '"+filePath+"' wasn't found", ex);
        } catch (IOException ex) {
            LOG.error("Error reading kryo registration file '"+filePath+"'", ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ex) {
                    LOG.warn("Error closing kryo registration file '"+filePath+"'", ex);
                }
            }
        }
        
        return result.toString();
    }
}
