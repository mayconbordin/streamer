package com.streamer.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import static com.streamer.utils.SamzaTaskConfiguration.*;
import java.io.ByteArrayOutputStream;
import org.apache.samza.config.Config;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SamzaKryoSerdeFactory<T> implements SerdeFactory<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SamzaKryoSerdeFactory.class);

    public Serde<T> getSerde(String string, Config config) {
        return new SamzaKryoSerde<T>(config.get(SERDE_REGISTRATION_KEY));
    }
    
    public static class SamzaKryoSerde<V> implements Serde<V> {
        private Kryo kryo;

        public SamzaKryoSerde(String registrationInfo) {
            kryo = new Kryo();
            register(registrationInfo);
        }
        
        private void register(String registrationInfo) {
            if (registrationInfo == null) return;
            
            String[] infoList = registrationInfo.split(COMMA);
            
            Class targetClass     = null;
            Class serializerClass = null;
            Serializer serializer = null;
            
            for (String info : infoList) {
                String[] fields = info.split(COLON);
                
                targetClass     = null;
                serializerClass = null;
                
                if (fields.length > 0) {
                    try {
                        targetClass = Class.forName(fields[0].replace(QUESTION_MARK, DOLLAR_SIGN));
                    } catch (ClassNotFoundException ex) {
                        LOG.error("Class "+fields[0]+" declared for serialization does not exist.", ex);
                    }
                }
                
                if (fields.length > 1) {
                    try {
                        serializerClass = Class.forName(fields[1].replace(QUESTION_MARK, DOLLAR_SIGN));
                    } catch (ClassNotFoundException ex) {
                        LOG.error("Class "+fields[1]+" for serialization of "+fields[0]+" does not exist.", ex);
                    }
                }
                
                if (targetClass != null) {
                    if (serializerClass == null) {
                        kryo.register(targetClass);
                    } else {
                        serializer = resolveSerializerInstance(kryo, targetClass, (Class<? extends Serializer>)serializerClass);
                        kryo.register(targetClass, serializer);
                    }
                } else {
                    LOG.warn("Invalid serialization declaration: {}", info);
                }
            }
        }
        
        private static Serializer resolveSerializerInstance(Kryo k, Class superClass, Class<? extends Serializer> serializerClass) {
            try {
                try {
                    return serializerClass.getConstructor(Kryo.class, Class.class).newInstance(k, superClass);
                } catch (Exception ex1) {
                    try {
                        return serializerClass.getConstructor(Kryo.class).newInstance(k);
                    } catch (Exception ex2) {
                        try {
                            return serializerClass.getConstructor(Class.class).newInstance(superClass);
                        } catch (Exception ex3) {
                            return serializerClass.newInstance();
                        }
                    }
                }
            } catch (Exception ex) {
                throw new IllegalArgumentException("Unable to create serializer '"
                        + serializerClass.getName() + "' for class '"
                        + superClass.getName() + "'", ex);
            }
        }

        public byte[] toBytes(V obj) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            Output output = new Output(bos);
            
            kryo.writeClassAndObject(output, obj);
            
            output.flush();
            output.close();
            
            return bos.toByteArray();
        }

        public V fromBytes(byte[] bytes) {
            Input input = new Input(bytes);
            Object obj = kryo.readClassAndObject(input);
            input.close();
            
            return (V) obj;
        }
        
    }
    
}
