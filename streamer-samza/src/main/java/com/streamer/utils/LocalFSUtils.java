package com.streamer.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFSUtils {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFSUtils.class);
    
    public static boolean serializeObject(Object obj, String fn) {
        FileOutputStream fos   = null;
        ObjectOutputStream oos = null;
        
        try {
            fos = new FileOutputStream(fn);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(obj);
        } catch (FileNotFoundException e) {
            LOG.error("Unable to write object to " + fn, e);
            return false;
        } catch (IOException e) {
            LOG.error("Error while writing object to " + fn, e);
            return false;
        } finally {
            try {
                if (fos != null) fos.close();
                if (oos != null) oos.close();
            } catch (IOException e) {
                LOG.error("Unable to close output stream", e);
            }
        }

        return true;
    }

    public static Object deserializeObject(String filename) {
        LOG.info("Deserialize local file: {}", filename);
        
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        Object obj = null;
        
        try {
            fis = new FileInputStream(filename);
            ois = new ObjectInputStream(fis);
            obj = ois.readObject();
        } catch (IOException e) {
            LOG.error("Error reading object from " + filename, e);
        } catch (ClassNotFoundException e) {
            LOG.error("Error deserializing object from " + filename, e);
        } finally {
            try {
                if (fis != null) fis.close();
                if (ois != null) ois.close();
            } catch (IOException e) {
                LOG.error("Error closing input stream", e);
            }
        }

        return obj;
    }
}