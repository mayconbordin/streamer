package com.streamer.utils;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.FileSystems;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HDFSUtils.class);
    private static final String TEMP_FILE = "sytemTemp";
    private static final String TEMP_FILE_SUFFIX = ".dat";
    
    private static String coreConfPath;
    private static String hdfsConfPath;
    private static String configHomePath;
    private static String systemDir = null;
    private static org.apache.hadoop.conf.Configuration config;

    public static void setHadoopConfigHome(String hadoopConfPath) {
        LOG.info("Hadoop configuration home: {}", hadoopConfPath);
        configHomePath = hadoopConfPath;
        
        java.nio.file.Path coreSitePath = FileSystems.getDefault().getPath(hadoopConfPath, "core-site.xml");
        java.nio.file.Path hdfsSitePath = FileSystems.getDefault().getPath(hadoopConfPath, "hdfs-site.xml");
        
        coreConfPath = coreSitePath.toAbsolutePath().toString();
        hdfsConfPath = hdfsSitePath.toAbsolutePath().toString();
    }
    
    public static org.apache.hadoop.conf.Configuration getConfig() {
        if (config == null) {
            config = new org.apache.hadoop.conf.Configuration();
            config.addResource(new Path(coreConfPath));
            config.addResource(new Path(hdfsConfPath));
            
            config.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
            config.set("fs.file.impl", LocalFileSystem.class.getName());
        }
        
        return config;
    }

    public static String getNameNodeUri() {
        return getConfig().get("fs.defaultFS");
    }

    public static String getHadoopConfigHome() {
        return configHomePath;
    }

    public static void setSystemDir(String dir) {
        LOG.info("Hadoop system directory: {}", dir);
        
        if (dir != null)
            systemDir = getNameNodeUri() + Path.SEPARATOR + dir + Path.SEPARATOR;
        else 
            systemDir = null;
    }

    public static String getDefaultSystemDir() throws IOException {
        FileSystem fs = FileSystem.get(getConfig());
        Path defaultDir = new Path(fs.getHomeDirectory(), ".samoa");
        return defaultDir.toString();
    }

    public static boolean deleteFileIfExist(String absPath) {
        Path p = new Path(absPath);
        return deleteFileIfExist(p);
    }

    public static boolean deleteFileIfExist(Path p) {
        FileSystem fs;
        try {
            fs = FileSystem.get(getConfig());
            if (fs.exists(p)) {
                return fs.delete(p, false);
            } else {
                return true;
            }
        } catch (IOException e) {
            LOG.error("Unable to delete " + p.getName(), e);
        }
        return false;
    }

    public static String writeToHDFS(File file, String dstPath) {
        LOG.info("Filesystem name: {}", getConfig().get("fs.defaultFS"));

        // Default samoaDir
        if (systemDir == null) {
            try {
                systemDir = getDefaultSystemDir();
            } catch (IOException ex) {
                LOG.error("Unable to get default system directory", ex);
                return null;
            }
        }

        // Setup src and dst paths
        //java.nio.file.Path tempPath = FileSystems.getDefault().getPath(systemDir, dstPath);
        Path dst = new Path(systemDir, dstPath);
        Path src = new Path(file.getAbsolutePath());

        // Delete file if already exists in HDFS
        if (deleteFileIfExist(dst) == false)
            return null;

        // Copy to HDFS
        FileSystem fs;
        try {
            fs = FileSystem.get(getConfig());
            fs.copyFromLocalFile(src, dst);
        } catch (IOException ex) {
            LOG.error("Unable to copy file to HDFS", ex);
            return null;
        }

        return dst.toString(); // abs path to file
    }

    public static Object deserializeObject(String filePath) {
        LOG.info("Deserialize hdfs://{}", filePath);

        Path file = new Path(filePath);
        FSDataInputStream dataInputStream = null;
        ObjectInputStream ois = null;
        Object obj = null;
        FileSystem fs;
        
        try {
            fs = FileSystem.get(getConfig());
            dataInputStream = fs.open(file);
            ois = new ObjectInputStream(dataInputStream);
            obj = ois.readObject();
        } catch (IOException ex) {
            LOG.error("Error while deserializing object", ex);
        } catch (ClassNotFoundException e) {
            LOG.error("Unable to read deserialized object", e);
            try {
                if (dataInputStream != null) dataInputStream.close();
                if (ois != null) ois.close();
            } catch (IOException ex) {
                LOG.error("Unable to close input stream", ex);
            }
        }

        return obj;
    }
    
    public static String serializeObject(Object object, String path) {
            LOG.info("Serialize object {} to hdfs://{}", object.getClass().getName(), path);
            try {
                File tmpDatFile = File.createTempFile(TEMP_FILE, TEMP_FILE_SUFFIX);
                
                if (LocalFSUtils.serializeObject(object, tmpDatFile.getAbsolutePath())) {
                    return HDFSUtils.writeToHDFS(tmpDatFile, path);
                }
            } catch (IOException e) {
                LOG.error("Unable to serialize object", e);
            }
            return null;
    }
}