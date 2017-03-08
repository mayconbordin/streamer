package com.streamer.examples.voipstream;


import com.streamer.core.Operator;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterBolt extends Operator {
    private static final Logger LOG = Logger.getLogger(AbstractFilterBolt.class);
    
    //protected OutputCollector collector;
    //protected ODTDBloomFilter filter;
    /*
    protected String configPrefix;
    protected String outputField;

    public AbstractFilterBolt(String configPrefix, String outputField) {
        this.configPrefix = configPrefix;
        this.outputField = outputField;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(CALLING_NUM_FIELD, TIMESTAMP_FIELD, outputField, RECORD_FIELD));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        
        // parameters
        int numElements       = ConfigUtility.getInt(stormConf, "voipstream." + configPrefix + ".num_elements");
        int bucketsPerElement = ConfigUtility.getInt(stormConf, "voipstream." + configPrefix + ".buckets_per_element");
        int bucketsPerWord    = ConfigUtility.getInt(stormConf, "voipstream." + configPrefix + ".buckets_per_word");
        double beta           = ConfigUtility.getDouble(stormConf, "voipstream." + configPrefix + ".beta");
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }*/
}