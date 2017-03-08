package com.streamer.utils;

import java.util.Map;

public class FileSystemUtils {
    public static final String HDFS = "hdfs";
    public static final String LOCAL_FS = "local";
        
    public static Map<String,Object> deserializeMapFromFile(String filesystem, String filename) {
        Map<String,Object> map;
        if (filesystem.equals(HDFS)) {
            map = (Map<String,Object>) HDFSUtils.deserializeObject(filename);
        } else {
            map = (Map<String,Object>) LocalFSUtils.deserializeObject(filename);
        }
        return map;
    }

    public static Object deserializeObjectFromFileAndKey(String filesystem, String filename, String key) {
        Map<String, Object> map = deserializeMapFromFile(filesystem, filename);
        if (map == null) return null;
        return map.get(key);
    }
    
    public static <T> T deserialize(String filesystem, String filename, Class<T> clazz) {
        Object obj;
        if (filesystem.equals(HDFS)) {
            obj = HDFSUtils.deserializeObject(filename);
        } else {
            obj = LocalFSUtils.deserializeObject(filename);
        }
        
        try {
            return clazz.cast(obj);
        } catch(ClassCastException e) {
            return null;
        }
    }
}
